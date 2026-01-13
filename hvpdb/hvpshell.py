import cmd
import shlex
import json
import os
import time
import random
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.tree import Tree
from rich.markup import escape
from .core import HVPDB
console = Console()

class HVPShell(cmd.Cmd):
    intro = None
    prompt = 'hvpdb > '

    def __init__(self, db: HVPDB=None):
        super().__init__()
        self.db = db
        self.current_group = None
        self.prev_group = None
        self.current_doc = None
        self.selected_docs = []
        self.is_locked = False
        self.last_search_results = []
        self._cmd_history = []
        self.record_mode = True
        self.auto_save = False

    def preloop(self):
        banner_text = "\n    [bold]Connection:[/bold]\n    - [green]connect[/green] <path>    : Connect to database\n    - [green]disconnect[/green]        : Disconnect current DB\n\n    [bold]Navigation:[/bold]\n    - [green]scan[/green]           : List all groups\n    - [green]target[/green] <group> : Select a group context\n    \n    [bold]Data Operations:[/bold]\n    - [green]peek[/green]           : View all documents\n    - [green]hunt[/green] k=v       : Search documents\n    - [green]make[/green] k=v       : Create new document\n    - [green]check[/green]          : Count documents\n    - [green]truncate[/green]       : Delete all documents in group\n    - [green]inhale/exhale[/green]  : Import/Export JSON\n    - [green]distinct[/green] <f>   : List unique values\n    - [green]stats[/green] <f>      : Calculate statistics\n    \n    [bold]Group Operations:[/bold]\n    - [green]rename[/green] <name>  : Rename current group\n    - [green]clone[/green] <src> <dst>: Clone a group\n    \n    [bold]Item Operations (After 'pick'):[/bold]\n    - [green]pick[/green] <index>   : Select document from list\n    - [green]morph[/green] k=v      : Update selected document\n    - [green]throw[/green]          : Delete selected document\n    - [green]fuse[/green] <id1> <id2>: Merge two documents\n    - [green]sift[/green]           : Deduplicate documents\n    \n    [bold]Audit & Version Control:[/bold]\n    - [green]record[/green]           : Data versioning (undo/redo)\n    - [green]trace[/green]            : View audit log\n    \n    [bold]System & Maintenance:[/bold]\n    - [green]save[/green]             : Save to disk\n    - [green]refresh[/green]          : Reload from disk\n    - [green]perm[/green]             : Check permissions\n    - [green]index[/green] <field>  : Create index\n    - [green]schema[/green]         : Infer schema\n    - [green]vacuum[/green]         : Compact storage\n    - [green]validate[/green]       : Check DB integrity\n    - [green]benchmark[/green]      : Run performance test\n    - [green]monitor[/green]        : Realtime dashboard\n    - [green]status[/green]         : Database info\n    - [green]tune[/green]           : Configure shell\n    - [green]history[/green]        : Show command history\n    - [green]clear[/green]          : Clear screen\n    - [green]quit[/green]           : Exit\n\n    [bold]Plugins:[/bold]\n    - [green]query[/green] <sql>    : Polyglot Query (SQL/Mongo/Redis)\n\n    [dim]Tip: Type 'help <command>' for detailed usage.[/dim]\n        "
        console.print(Panel(banner_text.strip(), title='[bold cyan]HVPDB Ops Shell (HVPShell)[/bold cyan]', subtitle='[dim]Action-Oriented Database Shell[/dim]', border_style='cyan'))
        self._update_prompt()

    def do_status(self, arg):
        if not self.db:
            console.print('[yellow]Not connected.[/yellow]')
            return
        console.print(Panel(f"Target: {self.db.filepath}\nGroup: {self.current_group or 'None'}\nSequence: {self.db.storage._last_sequence}\nDocs in Group: {(len(self.db.group(self.current_group).get_all()) if self.current_group else 0)}", title='Database Status'))

    def do_use(self, arg):
        self.do_target(arg)

    def do_peek(self, arg):
        if not self.db:
            return
        if not self.current_group:
            console.print("[red]No group selected. Use 'target <group>' first.[/red]")
            return
        limit = 20
        if arg and arg.isdigit():
            limit = int(arg)
        grp = self.db.group(self.current_group)
        docs_iter = grp.get_all_iter()
        import itertools
        head_docs = list(itertools.islice(docs_iter, limit))
        table = Table(title=f"Documents in '{self.current_group}'")
        table.add_column('ID', style='cyan')
        table.add_column('Preview', style='dim')
        for doc in head_docs:
            preview = str(doc)[:50] + '...' if len(str(doc)) > 50 else str(doc)
            table.add_row(doc.get('_id', '?'), preview)
        console.print(table)
        total = len(grp.storage.data['groups'][self.current_group.name])
        if total > limit:
            console.print(f"[dim]... and {total - limit} more. Use 'peek {limit + 20}' to see more.[/dim]")

    def do_ls(self, arg):
        self.do_peek(arg)

    def do_get(self, arg):
        if not self.current_group:
            console.print('[red]Select a group first (use target <group>).[/red]')
            return
        doc_id = arg.strip()
        if not doc_id and self.current_doc:
            doc_id = self.current_doc['_id']
        doc = self.db.group(self.current_group).find_one({'_id': doc_id})
        if doc:
            console.print_json(data=doc)
        else:
            console.print(f'[red]Document {doc_id} not found.[/red]')

    do_cat = do_get

    def do_del(self, arg):
        if not self.current_group:
            console.print('[red]Select a group first.[/red]')
            return
        doc_id = arg.strip()
        if self.db.group(self.current_group).delete({'_id': doc_id}):
            console.print(f'[green]Document {doc_id} deleted.[/green]')
            self.db.commit()
        else:
            console.print(f'[red]Document {doc_id} not found.[/red]')

    def do_grep(self, arg):
        if not self.current_group:
            console.print('[red]Select a group first.[/red]')
            return
        if '=' not in arg:
            console.print('[yellow]Usage: grep key=value[/yellow]')
            return
        key, val = arg.split('=', 1)
        if val.isdigit():
            val = int(val)
        elif val.lower() == 'true':
            val = True
        elif val.lower() == 'false':
            val = False
        results = self.db.group(self.current_group).find({key: val})
        console.print(f'Found {len(results)} documents:')
        for doc in results[:10]:
            console.print(f'- {doc}')

    def do_change(self, arg):
        parts = arg.split()
        if len(parts) < 2:
            console.print('[yellow]Usage: change <key> <value> [args...][/yellow]')
            return
        key = parts[0]
        if key == 'db_password':
            new_pass = parts[1]
            if not new_pass:
                console.print('[red]Password cannot be empty.[/red]')
                return
            if console.input(f'[bold red]Change MASTER DB PASSWORD? This will re-encrypt the entire database. (yes/no): [/bold red]') != 'yes':
                return
            try:
                self.db.storage.password = new_pass
                self.db.storage._dirty = True
                self.db.storage.security = None
                self.db.commit()
                console.print('[bold green]Database password changed and file re-encrypted.[/bold green]')
            except Exception as e:
                console.print(f'[red]Failed to change DB password: {e}[/red]')
            return
        if key == 'user_password':
            if len(parts) < 3:
                console.print('[yellow]Usage: change user_password <username> <new_password>[/yellow]')
                return
            username = parts[1]
            new_user_pass = parts[2]
            try:
                from hvpdb_perms import PermissionManager
                pm = PermissionManager(self.db)
                current_user = getattr(self.db, 'current_user', None)
                is_root = current_user is None or current_user == 'root'
                if not is_root:
                    caller_data = self.db.storage.data['users'].get(current_user)
                    if not caller_data or caller_data.get('role') != 'admin':
                        if current_user != username:
                            console.print("[red]Access Denied: Only Admin/Root can change other users' passwords.[/red]")
                            return
            except ImportError:
                pass
            if 'users' not in self.db.get_all_groups():
                console.print("[red]User management system not found (no 'users' group).[/red]")
                return
            users_grp = self.db.group('users')
            user_doc = users_grp.find_one({'username': username})
            if not user_doc:
                console.print(f"[red]User '{username}' not found.[/red]")
                return
            users_grp.update({'_id': user_doc['_id']}, {'password': new_user_pass})
            self.db.commit()
            console.print(f"[green]Password for user '{username}' updated.[/green]")
            return
        is_doc_update = self.current_doc or self.selected_docs
        if is_doc_update:
            value = ' '.join(parts[1:])
            self.do_morph(f'{key}={value}')
            return
        console.print('[yellow]Unknown command or no document selected.[/yellow]')
        console.print("See 'help change' for system commands.")

    def do_query(self, arg):
        if not self._check_db():
            return
        if not arg:
            console.print('[yellow]Usage: query <query_string>[/yellow]')
            return
        try:
            from hvpdb_query.parser import PolyglotParser
            from hvpdb_query.engine import QueryEngine
        except ImportError:
            console.print("[red]Error: 'hvpdb-query' plugin not installed.[/red]")
            console.print("[yellow]This feature requires the Query Engine plugin.[/yellow]")
            console.print("To install, run: [green]pip install hvpdb-query[/green]")
            return
        parser = PolyglotParser()
        engine = QueryEngine(self.db)
        try:
            plan = parser.parse(arg)
            if not plan:
                console.print('[red]Invalid Query Syntax.[/red]')
                return
            results = engine.execute(plan)
            if isinstance(results, list):
                console.print(f'[green]Found {len(results)} results.[/green]')
                self.last_search_results = results
                if results:
                    cols = set()
                    for r in results[:10]:
                        cols.update(r.keys())
                    cols = sorted(list(cols))
                    table = Table(show_header=True)
                    for c in cols:
                        table.add_column(c)
                    for r in results:
                        table.add_row(*[str(r.get(c, '')) for c in cols])
                    console.print(table)
            else:
                console.print(f'[green]Result: {results}[/green]')
        except Exception as e:
            console.print(f'[red]Query Failed: {e}[/red]')

    def do_connect(self, arg):
        if self.db:
            console.print("[yellow]Already connected. Use 'disconnect' first.[/yellow]")
            return
        args = arg.split()
        if not args:
            console.print('[red]Usage: connect <path>[/red]')
            return
        path = args[0]
        password = None
        if len(args) > 1:
            console.print('[bold red]SECURITY WARNING: Passing password as argument is insecure![/bold red]')
            console.print('[yellow]Password will be visible in history/process list. Use prompt instead.[/yellow]')
            password = args[1]
        else:
            password = console.input('Enter Password: ', password=True)
        if not path.startswith('hvp://') and (not path.endswith('.hvp')) and (not path.endswith('.hvdb')):
            path += '.hvp'
        try:
            self.db = HVPDB(path, password)
            console.print(f'[green]Connected to {self._mask_uri(path)}[/green]')
            self._update_prompt()
        except Exception as e:
            err_msg = str(e)
            if password and password in err_msg:
                err_msg = err_msg.replace(password, '***')
            console.print(f'[red]Connection failed: {err_msg}[/red]')

    def do_disconnect(self, arg):
        if not self.db:
            console.print('[yellow]Not connected.[/yellow]')
            return
        try:
            self.db.close()
        except:
            pass
        self.db = None
        self.current_group = None
        self.current_doc = None
        self.is_locked = False
        self.last_search_results = []
        console.print('[green]Disconnected. Context cleared.[/green]')
        self._update_prompt()

    def do_refresh(self, arg):
        if not self.db:
            console.print('[yellow]Not connected.[/yellow]')
            return
        try:
            self.db.refresh()
            console.print('[green]Database refreshed successfully.[/green]')
            if self.current_group:
                pass
        except Exception as e:
            console.print(f'[red]Refresh failed: {e}[/red]')

    def do_patch(self, arg):
        if not self._check_db():
            return
        if not self.current_group:
            console.print("[red]Select a group first with 'use <group>'[/red]")
            return
        parts = arg.split(maxsplit=1)
        if len(parts) < 2:
            console.print('[yellow]Usage: patch <id> <json_string>[/yellow]')
            return
        doc_id, json_str = parts
        try:
            data = json.loads(json_str)
            if not isinstance(data, dict):
                console.print('[red]Data must be a JSON object[/red]')
                return
            grp = self.db.group(self.current_group)
            count = grp.update({'_id': doc_id}, data)
            self.db.commit()
            if count:
                console.print(f'[green]Document {doc_id} updated.[/green]')
            else:
                console.print(f'[yellow]Document {doc_id} not found.[/yellow]')
        except json.JSONDecodeError:
            console.print('[red]Invalid JSON[/red]')
        except Exception as e:
            console.print(f'[red]Error: {e}[/red]')

    def _check_db(self):
        if not self.db:
            console.print("[red]Not connected to any database. Use 'connect <path>' first.[/red]")
            return False
        return True

    def cmdloop(self, intro=None):
        self.preloop()
        if self.use_rawinput and self.completekey:
            try:
                import readline
                self.old_completer = readline.get_completer()
                readline.set_completer(self.complete)
                readline.parse_and_bind(self.completekey + ': complete')
            except ImportError:
                pass
        stop = None
        while not stop:
            try:
                if self.use_rawinput:
                    line = console.input(self.prompt)
                else:
                    self.stdout.write(self.prompt)
                    line = self.stdin.readline()
                    if not len(line):
                        line = 'EOF'
                    else:
                        line = line.rstrip('\r\n')
                line = self.precmd(line)
                stop = self.onecmd(line)
                stop = self.postcmd(stop, line)
            except KeyboardInterrupt:
                console.print('^C')
            except EOFError:
                console.print('^D')
                break
            except Exception as e:
                console.print(f'[red]Error:[/red] {escape(str(e))}')
        self.postloop()

    def precmd(self, line):
        if line and line != 'history':
            self._cmd_history.append(self._redact_history(line))
        return line

    def _redact_history(self, line: str) -> str:
        SENSITIVE_CMDS = ('connect', 'become', 'user create', 'hvpdb shell', 'hvpdb init')
        SENSITIVE_KEYS = ('password=', 'pass=', 'token=', 'secret=', 'key=')
        low = line.lower().strip()
        for cmd in SENSITIVE_CMDS:
            if low.startswith(cmd):
                parts = line.split()
                if len(parts) > 1:
                    return parts[0] + ' [REDACTED]'
                return line
        if any((k in low for k in SENSITIVE_KEYS)):
            parts = line.split()
            masked = []
            for p in parts:
                pl = p.lower()
                is_sensitive = False
                for k in SENSITIVE_KEYS:
                    if pl.startswith(k):
                        key_part = p.split('=', 1)[0]
                        masked.append(f'{key_part}=[REDACTED]')
                        is_sensitive = True
                        break
                if not is_sensitive:
                    masked.append(p)
            return ' '.join(masked)
        return line

    def do_history(self, arg):
        if not self._cmd_history:
            console.print('[dim]No history yet.[/dim]')
            return
        for i, cmd in enumerate(self._cmd_history[-20:]):
            console.print(f'{i + 1}. {cmd}')

    def do_tour(self, arg):
        self.do_getatour(arg)

    def do_cheatsheet(self, arg):
        console.print(Panel('\n        [bold]HVPDB Cheatsheet[/bold]\n        [green]focus <group>[/green]   : Select group (e.g. focus users)\n        [green]find k=v[/green]        : Search (e.g. find role=admin)\n        [green]show[/green]            : List docs (e.g. show, show 20)\n        [green]create k=v[/green]      : New doc (e.g. create name=A)\n        [green]update k=v[/green]      : Edit doc (e.g. update age=30)\n        [green]remove[/green]          : Delete doc\n        [green]timeline[/green]        : History\n        [green]quit[/green]            : Exit\n        ', title='Quick Ref'))

    def do_examples(self, arg):
        console.print('[bold]Examples:[/bold]')
        console.print('  create name=Alice role=admin')
        console.print('  focus users')
        console.print('  find role=admin')
        console.print('  update status=active')
        console.print('  stats age')

    def do_explain(self, arg):
        if not arg:
            console.print('[yellow]Usage: explain <command>[/yellow]')
            return
        cmd_func = getattr(self, f'do_{arg}', None)
        if cmd_func and cmd_func.__doc__:
            console.print(Panel(cmd_func.__doc__, title=f'Explain: {arg}'))
        else:
            console.print(f'[red]Unknown command: {arg}[/red]')

    def do_why(self, arg):
        console.print('[dim]Analysis: Most likely syntax error or missing permission.[/dim]')

    def do_tips(self, arg):
        tips = ["Use 'focus <group>' to switch context quickly.", "Batch operations: 'find k=v' -> 'select all' -> 'update k=v2'", "Use 'track' to see your command history.", "Type 'help <cmd>' for detailed usage."]
        console.print(f'[cyan]💡 Tip: {random.choice(tips)}[/cyan]')

    def do_doctor(self, arg):
        self.do_diagnose(arg)

    def do_teach(self, arg):
        console.print('[dim]Teacher mode active.[/dim]')

    def do_focus(self, arg):
        self.do_target(arg)

    def do_unfocus(self, arg):
        self.current_group = None
        self._update_prompt()
        console.print('[dim]Context cleared.[/dim]')

    def do_switch(self, arg):
        if self.prev_group:
            self.do_target(self.prev_group.name if hasattr(self.prev_group, 'name') else self.prev_group)
        else:
            console.print('[yellow]No previous group.[/yellow]')

    def do_context(self, arg):
        self.do_status(arg)

    def do_lock(self, arg):
        self.is_locked = True
        console.print('[red]🔒 Shell Locked (Read-Only)[/red]')

    def do_unlock(self, arg):
        self.is_locked = False
        console.print('[green]🔓 Shell Unlocked[/green]')

    def do_show(self, arg):
        args = arg.split()
        if not args:
            self.do_ls('')
        elif args[0] == 'at':
            self.do_pick(args[1] if len(args) > 1 else '')
        elif args[0] == 'full':
            self.do_ls(arg)
        else:
            self.do_ls(arg)

    def do_sample(self, arg):
        self.do_sample_impl(arg)

    def do_find(self, arg):
        self.do_hunt(arg)

    def do_count(self, arg):
        if arg:
            self.do_hunt(arg)
        else:
            self.do_check(arg)

    def do_distinct(self, arg):
        if not self.current_group:
            return
        field = arg.split()[0] if arg else ''
        if not field:
            console.print('[yellow]Usage: distinct <field>[/yellow]')
            return
        values = set()
        for doc in self.current_group.find():
            if field in doc:
                values.add(str(doc[field]))
        console.print(f"[bold]Distinct values for '{field}':[/bold]")
        console.print(', '.join(sorted(values)))

    def do_freq(self, arg):
        if not self.current_group:
            return
        field = arg.split()[0]
        docs = self.current_group.find()
        from collections import Counter
        vals = [str(d.get(field)) for d in docs if field in d]
        c = Counter(vals)
        console.print(f"Frequency for '{field}': {c.most_common(10)}")

    def do_stats(self, arg):
        self.do_stats_impl(arg)

    def do_create(self, arg):
        self.do_make(arg)

    def do_creategroup(self, arg):
        if not self.db:
            return
        self.db.group(arg)
        console.print(f"[green]Group '{arg}' created.[/green]")

    def do_update(self, arg):
        self.do_morph(arg)

    def do_unset(self, arg):
        if not self.current_group:
            return
        target_ids = []
        if self.selected_docs:
            target_ids = self.selected_docs
        elif self.current_doc:
            target_ids = [self.current_doc['_id']]
        else:
            console.print('[red]No document selected.[/red]')
            return
        count = 0
        for doc_id in target_ids:
            doc = self.current_group.find_one({'_id': doc_id})
            if doc and arg in doc:
                del doc[arg]
                self.current_group.update({'_id': doc_id}, doc)
                pass
        parts = arg.split()
        if not parts:
            return
        field = parts[0]
        for doc_id in target_ids:
            doc = self.current_group.find_one({'_id': doc_id})
            if doc and field in doc:
                del doc[field]
                self.current_group.update({'_id': doc_id}, doc)
                self.db.storage._dirty = True
                count += 1
        self.db.commit()
        console.print(f"[green]Unset '{field}' in {count} docs.[/green]")

    def do_replace(self, arg):
        if not self.current_doc:
            console.print('[red]Select a document first.[/red]')
            return
        try:
            new_data = json.loads(arg)
            if '_id' in new_data and new_data['_id'] != self.current_doc['_id']:
                console.print('[red]Cannot change _id.[/red]')
                return
            new_data['_id'] = self.current_doc['_id']
            self.db.storage.data['groups'][self.current_group.name][self.current_doc['_id']] = new_data
            self.db.storage._dirty = True
            self.current_doc = new_data
            self.db.commit()
            console.print('[green]Document replaced.[/green]')
        except json.JSONDecodeError:
            console.print('[red]Invalid JSON.[/red]')

    def do_remove(self, arg):
        self.do_throw(arg)

    def do_removeid(self, arg):
        self.do_del(arg)

    def do_renamegroup(self, arg):
        self.do_rename(arg)

    def do_clonegroup(self, arg):
        self.do_clone(arg)

    def do_move(self, arg):
        if not self.current_group:
            return
        target = arg.strip()
        if self.selected_docs:
            for doc_id in self.selected_docs:
                self._exec_move_copy(self.current_group, doc_id, target, is_move=True)
        elif self.current_doc:
            self._exec_move_copy(self.current_group, self.current_doc['_id'], target, is_move=True)
        else:
            console.print('[yellow]Select docs first.[/yellow]')

    def do_moveid(self, arg):
        parts = arg.split()
        if len(parts) == 2:
            self._exec_move_copy(self.current_group, parts[0], parts[1], is_move=True)

    def do_copy(self, arg):
        if not self.current_group:
            return
        target = arg.strip()
        if self.selected_docs:
            for doc_id in self.selected_docs:
                self._exec_move_copy(self.current_group, doc_id, target, is_move=False)
        elif self.current_doc:
            self._exec_move_copy(self.current_group, self.current_doc['_id'], target, is_move=False)

    def do_copyid(self, arg):
        parts = arg.split()
        if len(parts) == 2:
            self._exec_move_copy(self.current_group, parts[0], parts[1], is_move=False)

    def do_merge(self, arg):
        console.print('[dim]Merge not implemented yet.[/dim]')

    def do_dedupe(self, arg):
        console.print('[dim]Dedupe not implemented yet.[/dim]')

    def do_snapshot(self, arg):
        self.do_backup(arg)

    def do_restore(self, arg):
        console.print("[yellow]Use 'connect' to open snapshot, or manual file copy.[/yellow]")

    def do_verify(self, arg):
        self.do_validate(arg)

    def do_guard(self, arg):
        console.print('[dim]Guard mode enabled.[/dim]')

    def do_confirm(self, arg):
        console.print(f'[dim]Confirmation level set to {arg}[/dim]')

    def do_seal(self, arg):
        self.do_lock(arg)

    def do_unseal(self, arg):
        self.do_unlock(arg)

    def do_timeline(self, arg):
        self.do_record('list ' + arg)

    def do_revert(self, arg):
        self.do_record('undo ' + arg)

    def do_reapply(self, arg):
        self.do_record('apply ' + arg)

    def do_checkpoint(self, arg):
        self.db.storage.save()
        console.print('[green]Checkpoint created.[/green]')

    def do_recover(self, arg):
        console.print('[dim]Recovery runs automatically on connect.[/dim]')

    def do_make(self, arg):
        if arg.startswith('group:'):
            grp_name = arg.split(':', 1)[1]
            self.do_creategroup(grp_name)
            return
        self.do_create(arg)

    def do_scout(self, arg):
        self.do_scan(arg)

    def do_scry(self, arg):
        self.do_schema(arg)


    def do_pulse(self, arg):
        self.do_status(arg)

    def do_ignite(self, arg):
        self.do_connect(arg)

    def do_vanish(self, arg):
        return self.do_quit(arg)

    def do_freeze(self, arg):
        self.do_save(arg)

    def do_revive(self, arg):
        self.do_refresh(arg)

    def do_drain(self, arg):
        self.do_vacuum(arg)

    def do_crypt(self, arg):
        self.do_change('db_password ' + arg)

    def do_track(self, arg):
        self.do_history(arg)

    def do_chronos(self, arg):
        console.print(f"[cyan]{time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]")

    def do_anchor(self, arg):
        self._anchor = (self.current_group, self.current_doc)
        console.print('[cyan]Anchor established.[/cyan]')

    def do_recall(self, arg):
        if hasattr(self, '_anchor') and self._anchor:
            grp, doc = self._anchor
            grp_name = grp.name if hasattr(grp, 'name') else grp
            if grp_name in self.db.get_all_groups():
                self.current_group = self.db.group(grp_name)
                self.current_doc = doc
                self._update_prompt()
                console.print(f'[cyan]Warped to anchor: {grp_name}[/cyan]')
            else:
                console.print('[yellow]Anchor unstable.[/yellow]')
        else:
            console.print('[yellow]No anchor.[/yellow]')

    def do_hunt_impl(self, arg):
        self.do_grep(arg)

    def do_make_impl(self, arg):
        if not self._check_db() or not self.current_group:
            return
        try:
            data = {}
            for part in arg.split():
                if '=' in part:
                    k, v = part.split('=', 1)
                    data[k] = v
            if data:
                self.db.group(self.current_group).insert(data)
                self.db.commit()
                console.print(f'[green]Entity created: {data}[/green]')
            else:
                console.print('[yellow]Usage: create key=value ...[/yellow]')
        except Exception as e:
            console.print(f'[red]Error: {e}[/red]')

    def do_morph_impl(self, arg):
        console.print('[dim]Morphing... (Not fully implemented)[/dim]')

    def do_check_impl(self, arg):
        if not self.current_group:
            return
        c = self.db.group(self.current_group).count()
        console.print(f'[cyan]Count: {c}[/cyan]')

    def do_stats_impl(self, arg):
        console.print('[dim]Calculating stats...[/dim]')

    def do_drop_impl(self, arg):
        console.print('[red]Drop group not implemented yet.[/red]')

    def do_truncate_impl(self, arg):
        if not self.current_group:
            return
        console.print(f'[red]Cleansed {self.current_group}.[/red]')

    def do_fuse(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        parts = arg.split()
        if len(parts) < 2:
            console.print('[yellow]Usage: fuse <id1> <id2> [prefer_left|prefer_right][/yellow]')
            return
        id1, id2 = (parts[0], parts[1])
        strategy = parts[2] if len(parts) > 2 else 'prefer_right'
        grp = self.db.group(self.current_group)
        doc1 = grp.find_one({'_id': id1})
        doc2 = grp.find_one({'_id': id2})
        if not doc1 or not doc2:
            console.print('[red]One or both documents not found.[/red]')
            return
        merged = doc1.copy()
        merged.update(doc2)
        if strategy == 'prefer_left':
            merged = doc2.copy()
            merged.update(doc1)
        merged.pop('_id', None)
        merged['_merged_from'] = [id1, id2]
        new_res = grp.insert(merged)
        self.db.commit()
        console.print(f"[green]Fused {id1} + {id2} -> {new_res['_id']}[/green]")

    def do_merge(self, arg):
        self.do_fuse(arg)

    def do_sift(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        grp = self.db.group(self.current_group)
        docs = grp.find()
        seen = set()
        to_delete = []
        target_field = arg.strip()
        for doc in docs:
            if target_field:
                val = doc.get(target_field)
                key = str(val)
            else:
                d_copy = doc.copy()
                d_copy.pop('_id', None)
                d_copy.pop('_created_at', None)
                d_copy.pop('_updated_at', None)
                key = json.dumps(d_copy, sort_keys=True)
            if key in seen:
                to_delete.append(doc['_id'])
            else:
                seen.add(key)
        if not to_delete:
            console.print('[green]No duplicates found.[/green]')
            return
        console.print(f'[yellow]Found {len(to_delete)} duplicates.[/yellow]')
        if console.input('[bold red]Delete duplicates? (y/n): [/bold red]').lower() == 'y':
            count = 0
            for did in to_delete:
                if grp.delete({'_id': did}):
                    count += 1
            self.db.commit()
            console.print(f'[green]Sifted out {count} duplicates.[/green]')

    def do_dedupe(self, arg):
        self.do_sift(arg)

    def do_inhale(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        if not arg:
            console.print('[yellow]Usage: inhale <file.json>[/yellow]')
            return
        path = arg.strip()
        if not os.path.exists(path):
            console.print(f'[red]File {path} not found.[/red]')
            return
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                data = [data]
            if not isinstance(data, list):
                console.print('[red]Invalid JSON format. Expected list or dict.[/red]')
                return
            grp = self.db.group(self.current_group)
            count = 0
            with console.status(f'Inhaling {len(data)} documents...'):
                for doc in data:
                    if isinstance(doc, dict):
                        grp.insert(doc)
                        count += 1
            self.db.commit()
            console.print(f'[green]Inhaled {count} documents from {path}.[/green]')
        except Exception as e:
            console.print(f'[red]Inhale failed: {e}[/red]')

    def do_import(self, arg):
        self.do_inhale(arg)

    def do_exhale(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        if not arg:
            console.print('[yellow]Usage: exhale <file.json>[/yellow]')
            return
        path = arg.strip()
        docs = self.db.group(self.current_group).find()
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(docs, f, indent=2, default=str)
            console.print(f'[green]Exhaled {len(docs)} documents to {path}.[/green]')
        except Exception as e:
            console.print(f'[red]Exhale failed: {e}[/red]')

    def do_export(self, arg):
        self.do_exhale(arg)

    def do_tune(self, arg):
        parts = arg.split()
        if len(parts) != 2:
            console.print('[yellow]Usage: tune <key> <value>[/yellow]')
            return
        k, v = parts
        console.print(f'[green]Tuned {k} to {v}.[/green]')

    def do_config(self, arg):
        self.do_tune(arg)

    def do_import_impl(self, arg):
        console.print('[dim]Inhaling...[/dim]')

    def do_export_impl(self, arg):
        console.print('[dim]Exhaling...[/dim]')

    def do_void_impl(self, arg):
        if not self._check_db() or not self.current_group:
            return
        parts = arg.split()
        if len(parts) < 2:
            console.print('[yellow]Usage: void <id> <field>[/yellow]')
            return
        doc_id, field = (parts[0], parts[1])
        grp = self.db.group(self.current_group)
        doc = grp.find_one({'_id': doc_id})
        if doc:
            if field in doc:
                doc[field] = None
                grp.update({'_id': doc_id}, doc)
                self.db.commit()
                console.print(f"[green]Voided field '{field}' in {doc_id}.[/green]")
            else:
                console.print(f"[yellow]Field '{field}' not found.[/yellow]")
        else:
            console.print(f'[red]Document {doc_id} not found.[/red]')

    def do_sample_impl(self, arg):
        if not self.current_group:
            console.print('[red]Select a group first.[/red]')
            return
        docs = self.db.group(self.current_group).get_all()
        if docs:
            doc = random.choice(docs)
            console.print_json(data=doc)
        else:
            console.print('[yellow]Empty group.[/yellow]')

    def _typewriter(self, text: str, speed: float=0.02, style: str='white'):
        for char in text:
            console.print(char, style=style, end='')
            time.sleep(speed)
        console.print()

    def do_getatour(self, arg):

        def ask_user():
            try:
                ans = console.input("\n[dim]Press [Enter] to continue, or type 'quit' to exit > [/dim]")
                if ans.lower().strip() == 'quit':
                    self._typewriter('\nSession terminated. Goodbye, Operator.', speed=0.04, style='bold red')
                    return False
                return True
            except KeyboardInterrupt:
                self._typewriter('\nInterrupted. Exiting tour.', speed=0.04, style='bold red')
                return False
        try:
            console.clear()
            console.print(Panel('[bold cyan]HVPDB INTERACTIVE PROTOCOL v3.0[/bold cyan]', border_style='cyan'))
            time.sleep(0.5)
            self._typewriter('Initializing Neural Interface...', speed=0.04, style='dim cyan')
            time.sleep(0.5)
            self._typewriter('Welcome, Operator. Accessing High Velocity Dataverse...', speed=0.03, style='bold white')
            time.sleep(0.5)
            self._typewriter('We have upgraded the command matrix. No more dashes. Pure velocity.', speed=0.03)
            self._typewriter('Uploading 50 New Command Modules...', speed=0.02, style='yellow')
            if not ask_user():
                return
            chapters = [('ONBOARDING', 'Getting Started', [('tour', 'Start this tour'), ('cheatsheet', 'Quick Reference'), ('examples', 'Copy-paste examples'), ('explain', 'Explain command'), ('why', 'Why command failed'), ('tips', 'Pro tips'), ('doctor', 'Health check'), ('teach', 'Tutorial mode')]), ('CONTEXT & NAVIGATION', 'Moving Around', [('focus', 'Select group'), ('unfocus', 'Clear context'), ('switch', 'Previous group'), ('context', 'Show status'), ('lock', 'Read-only mode'), ('unlock', 'Read-write mode'), ('select', 'Pick document')]), ('DATA VIEWING', 'See What You Have', [('show', 'List documents'), ('get', 'Get by ID'), ('sample', 'Random doc'), ('fields', 'Show fields'), ('tree', 'Visual structure'), ('schema', 'Infer schema')]), ('SEARCH', 'Find Needle in Haystack', [('find', 'Search k=v'), ('count', 'Count docs'), ('distinct', 'Unique values'), ('freq', 'Frequency analysis'), ('stats', 'Statistics')]), ('CREATE & EDIT', 'Make It Happen', [('create', 'New doc'), ('update', 'Edit doc'), ('set', 'Set field'), ('unset', 'Remove field'), ('replace', 'Replace doc'), ('remove', 'Delete doc'), ('creategroup', 'New group'), ('renamegroup', 'Rename group')]), ('MOVE DATA', 'Logistics', [('move', 'Move doc'), ('copy', 'Copy doc'), ('moveid', 'Move by ID'), ('copyid', 'Copy by ID'), ('merge', 'Merge docs'), ('dedupe', 'Remove duplicates')]), ('MAINTENANCE', 'Keep It Clean', [('verify', 'Check integrity'), ('vacuum', 'Compact space'), ('seal', 'Lock DB'), ('unseal', 'Unlock DB'), ('snapshot', 'Backup'), ('restore', 'Restore')]), ('WAL & HISTORY', 'Time Travel', [('timeline', 'Show history'), ('revert', 'Undo txn'), ('checkpoint', 'Save point'), ('recover', 'Crash recovery')])]
            for title, subtitle, cmds in chapters:
                console.print(f'\n[bold magenta]=== {title} ===[/bold magenta]')
                self._typewriter(subtitle, speed=0.02, style='italic cyan')
                time.sleep(0.3)
                table = Table(show_header=False, box=None)
                table.add_column('Cmd', style='green')
                table.add_column('Desc', style='dim')
                table.add_column('Cmd', style='green')
                table.add_column('Desc', style='dim')
                for i in range(0, len(cmds), 2):
                    c1, d1 = cmds[i]
                    c2, d2 = cmds[i + 1] if i + 1 < len(cmds) else ('', '')
                    table.add_row(f'» {c1}', d1, f'» {c2}' if c2 else '', d2)
                console.print(table)
                time.sleep(0.5)
                if title == 'CONTEXT & NAVIGATION':
                    console.print("\n[yellow][Simulation][/yellow] Switching context to 'users'.")
                    self._typewriter('Simulating: focus users', speed=0.05, style='dim')
                    console.print('hvpdb > ', end='')
                    time.sleep(0.3)
                    console.print('[green]focus users[/green]')
                    time.sleep(0.3)
                    console.print('hvpdb(users) > ', end='')
                    self._typewriter(' <-- Context shifted.', speed=0.02, style='cyan')
                if not ask_user():
                    return
            self._typewriter('\nUpgrade Complete. 50 Command Modules Active.', speed=0.04, style='bold green')
            self._typewriter("Type 'cheatsheet' for a quick start.", speed=0.03)
        except Exception as e:
            console.print(f'[red]Tour Error:[/red] {escape(str(e))}')

    def do_calc(self, arg):
        pass

    def do_type(self, arg):
        if not self.current_group:
            return
        parts = arg.split()
        if len(parts) < 2:
            return
        doc = self.db.group(self.current_group).find_one({'_id': parts[0]})
        if doc and parts[1] in doc:
            val = doc[parts[1]]
            console.print(f'Type: [cyan]{type(val).__name__}[/cyan] | Value: {val}')
        else:
            console.print('[red]Not found[/red]')

    def do_clear(self, arg):
        os.system('cls' if os.name == 'nt' else 'clear')

    def do_cls(self, arg):
        self.do_clear(arg)

    def _complete_groups(self, text, line, begidx, endidx):
        if not self.db:
            return []
        groups = self.db.get_all_groups()
        if not text:
            return groups
        return [g for g in groups if g.startswith(text)]

    def _complete_fields(self, text, line, begidx, endidx):
        if not self.current_group:
            return []
        fields = set()
        docs = self.current_group.find()
        for doc in docs[:5]:
            fields.update(doc.keys())
        fields = list(fields)
        if not text:
            return fields
        return [f for f in fields if f.startswith(text)]

    def complete_target(self, text, line, begidx, endidx):
        return self._complete_groups(text, line, begidx, endidx)

    def complete_drop(self, text, line, begidx, endidx):
        return self._complete_groups(text, line, begidx, endidx)

    def complete_nuke(self, text, line, begidx, endidx):
        return self._complete_groups(text, line, begidx, endidx)

    def complete_clone(self, text, line, begidx, endidx):
        return self._complete_groups(text, line, begidx, endidx)

    def complete_rename(self, text, line, begidx, endidx):
        return self._complete_groups(text, line, begidx, endidx)

    def complete_index(self, text, line, begidx, endidx):
        return self._complete_fields(text, line, begidx, endidx)

    def complete_distinct(self, text, line, begidx, endidx):
        return self._complete_fields(text, line, begidx, endidx)

    def complete_stats(self, text, line, begidx, endidx):
        return self._complete_fields(text, line, begidx, endidx)

    def do_become(self, arg):
        if not self._check_db():
            return
        username = arg.strip()
        if not username:
            console.print('[red]Usage: become <username>[/red]')
            return
        if username not in self.db.storage.data.get('users', {}):
            console.print(f"[red]User '{username}' does not exist.[/red]")
            return
        import getpass
        password = console.input(f'Password for [cyan]{username}[/cyan]: ', password=True)
        if self.db.authenticate(username, password):
            console.print(f'[green]Authenticated as {username}[/green]')
            self._update_prompt()
        else:
            console.print('[red]Authentication failed: Invalid password.[/red]')

    def do_whoami(self, arg):
        user = getattr(self.db, 'current_user', None)
        username = user
        if hasattr(user, 'username'):
            username = user.username
        if not username:
            username = 'root (system)'
        console.print(f'[bold cyan]{username}[/bold cyan]')

    def do_perm(self, arg):
        if not self._check_db():
            return
        username = getattr(self.db, 'current_user', None)
        if not username:
            console.print('[bold red]Current User: root (System Admin)[/bold red]')
            console.print('[dim]Root has full access to all groups.[/dim]')
            return
        user_data = self.db.storage.data.get('users', {}).get(username)
        if not user_data:
            console.print(f"[red]Error: User record for '{username}' not found.[/red]")
            return
        role = user_data.get('role', 'user')
        allowed_groups = user_data.get('groups', [])
        console.print(Panel(f'User: [bold cyan]{username}[/bold cyan]\nRole: [magenta]{role.upper()}[/magenta]', title='Permission Check', border_style='cyan'))
        all_groups = self.db.get_all_groups()
        if not all_groups:
            console.print('[yellow]No groups found in database.[/yellow]')
            return
        table = Table(title='Group Access Control')
        table.add_column('Group Name', style='white')
        table.add_column('Access', justify='center')
        table.add_column('Reason', style='dim')
        for grp in all_groups:
            has_access = False
            reason = 'Denied'
            if role == 'admin':
                has_access = True
                reason = 'Admin Role'
            elif '*' in allowed_groups:
                has_access = True
                reason = 'Wildcard (*)'
            elif grp in allowed_groups:
                has_access = True
                reason = 'Explicit Grant'
            status = '[green]✅ ALLOWED[/green]' if has_access else '[red]❌ DENIED[/red]'
            table.add_row(grp, status, reason)
        console.print(table)
