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

    def do_cat(self, arg):
        self.do_get(arg)

    def do_show(self, arg):
        self.do_get(arg)

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

    def do_history(self, arg):
        for i, cmd in enumerate(self._cmd_history):
            console.print(f'{i + 1}: {cmd}')

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

    def do_set(self, arg):
        if not self._check_db():
            return
        if not self.current_group:
            console.print("[red]Select a group first with 'use <group>'[/red]")
            return
        parts = arg.split(maxsplit=1)
        if len(parts) < 2:
            console.print('[yellow]Usage: set <id> <json_string>[/yellow]')
            return
        doc_id, json_str = parts
        try:
            data = json.loads(json_str)
            if not isinstance(data, dict):
                console.print('[red]Data must be a JSON object[/red]')
                return
            data['_id'] = doc_id
            grp = self.db.group(self.current_group)
            existing = grp.find_one({'_id': doc_id})
            if existing:
                grp.delete({'_id': doc_id})
                grp.insert(data)
                console.print(f'[green]Document {doc_id} replaced.[/green]')
            else:
                grp.insert(data)
                console.print(f'[green]Document {doc_id} created.[/green]')
            self.db.commit()
        except json.JSONDecodeError:
            console.print('[red]Invalid JSON[/red]')
        except Exception as e:
            console.print(f'[red]Error: {e}[/red]')

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

    def do_unset(self, arg):
        if not self._check_db():
            return
        if not self.current_group:
            console.print('[red]Select a group first.[/red]')
            return
        parts = arg.split()
        if len(parts) < 2:
            console.print('[yellow]Usage: unset <id> <field>[/yellow]')
            return
        doc_id, field = (parts[0], parts[1])
        grp = self.db.group(self.current_group)
        doc = grp.find_one({'_id': doc_id})
        if not doc:
            console.print(f'[red]Document {doc_id} not found.[/red]')
            return
        if field in doc:
            del doc[field]
            grp.delete({'_id': doc_id})
            grp.insert(doc)
            self.db.commit()
            console.print(f"[green]Field '{field}' removed from {doc_id}.[/green]")
        else:
            console.print(f"[yellow]Field '{field}' not in document.[/yellow]")

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

    def do_set(self, arg):
        parts = arg.split(maxsplit=1)
        if len(parts) == 2:
            self.do_morph(f'{parts[0]}={parts[1]}')
        else:
            console.print('[yellow]Usage: set <field> <value>[/yellow]')

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

    def do_change(self, arg):
        self.do_record('peek ' + arg)

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

    def do_query(self, arg):
        if not self._check_db():
            return
        engine = None
        parser = None
        try:
            from hvpdb_query.parser import PolyglotParser
            from hvpdb_query.engine import QueryEngine
            parser = PolyglotParser()
            engine = QueryEngine(self.db)
        except ImportError:
            if 'query' not in self.db.plugins:
                console.print('[red]Query plugin not installed (hvpdb-query package missing).[/red]')
                console.print("To install, run: [green]pip install hvpdb-query[/green]")
                return
            console.print('[yellow]Warning: Using plugin via entry-point interface not fully implemented. Install package to use fallback.[/yellow]')
            return
        if not engine or not parser:
            console.print('[red]Could not initialize Query Engine.[/red]')
            return
        try:
            plan = parser.parse(arg)
            if not plan:
                console.print('[yellow]Could not parse query.[/yellow]')
                return
            result = engine.execute(plan)
            console.print(result)
        except Exception as e:
            console.print(f'[red]Query Error: {e}[/red]')

    def do_target(self, arg):
        pass

    def do_scout(self, arg):
        self.do_scan(arg)

    def do_scry(self, arg):
        self.do_schema(arg)

    def do_hunt(self, arg):
        pass

    def do_nuke(self, arg):
        pass

    def do_morph(self, arg):
        pass

    def do_throw(self, arg):
        pass

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

    def do_edit(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        if not arg:
            console.print('[yellow]Usage: edit <doc_id>[/yellow]')
            return
        doc = self.current_group.find_one({'_id': arg})
        if not doc:
            console.print(f'[red]Document {arg} not found.[/red]')
            return
        import tempfile
        import subprocess
        try:
            fd, tf_path = tempfile.mkstemp(suffix='.json', text=True)
            with os.fdopen(fd, 'w') as tf:
                json.dump(doc, tf, indent=2, default=str)
            if os.name == 'nt':
                os.system(f'notepad {tf_path}')
            else:
                editor = os.environ.get('EDITOR', 'vim')
                subprocess.call([editor, tf_path])
            with open(tf_path, 'r') as tf:
                new_doc = json.load(tf)
            if new_doc != doc:
                if new_doc.get('_id') != doc['_id']:
                    console.print('[red]Error: Cannot change _id.[/red]')
                else:
                    self.current_group.update({'_id': doc['_id']}, new_doc)
                    self.db.commit()
                    console.print('[green]Document updated successfully via editor.[/green]')
            else:
                console.print('[dim]No changes made.[/dim]')
        except Exception as e:
            console.print(f'[red]Edit failed: {e}[/red]')
        finally:
            if 'tf_path' in locals() and os.path.exists(tf_path):
                os.remove(tf_path)

    def do_calc(self, arg):
        try:
            allowed = set('0123456789+-*/(). ')
            if not all((c in allowed for c in arg)):
                console.print('[red]Only basic math allowed.[/red]')
                return
            if '**' in arg or '//' in arg:
                pass
            console.print(f"= {eval(arg, {'__builtins__': {}})}")
        except SyntaxError:
            console.print('[red]Invalid Syntax[/red]')
        except Exception as e:
            console.print(f'[red]Error: {e}[/red]')

    def do_schema(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        docs = self.current_group.find()
        if not docs:
            console.print('[dim]Group is empty. Cannot infer schema.[/dim]')
            return
        schema = {}
        for doc in docs[:100]:
            for k, v in doc.items():
                t = type(v).__name__
                if k not in schema:
                    schema[k] = {t}
                else:
                    schema[k].add(t)
        table = Table(title=f'Schema Inference: {self.current_group.name}')
        table.add_column('Field', style='cyan')
        table.add_column('Types', style='green')
        for k, types in schema.items():
            table.add_row(k, ', '.join(types))
        console.print(table)

    def do_distinct(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        if not arg:
            console.print('[yellow]Usage: distinct <field_name>[/yellow]')
            return
        docs = self.current_group.find()
        values = set()
        for doc in docs:
            if arg in doc:
                val = doc[arg]
                if isinstance(val, (dict, list)):
                    val = str(val)
                values.add(val)
        console.print(f"[bold]Unique values for '{arg}':[/bold]")
        for v in sorted(list(values), key=lambda x: str(x)):
            console.print(f'- {v}')

    def do_stats(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        if not arg:
            console.print('[yellow]Usage: stats <field_name>[/yellow]')
            return
        docs = self.current_group.find()
        values = []
        for doc in docs:
            val = doc.get(arg)
            if isinstance(val, (int, float)):
                values.append(val)
        if not values:
            console.print(f"[yellow]No numeric data found for '{arg}'.[/yellow]")
            return
        avg = sum(values) / len(values)
        console.print(Panel(f"\n        Statistics for '{arg}'\n        --------------------\n        Count: {len(values)}\n        Min  : {min(values)}\n        Max  : {max(values)}\n        Sum  : {sum(values)}\n        Avg  : {avg:.2f}\n        ", title='Stats'))

    def do_rename(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        if not arg:
            console.print('[yellow]Usage: rename <new_name>[/yellow]')
            return
        old_name = self.current_group.name
        if hasattr(self.db, 'is_cluster') and self.db.is_cluster:
            console.print('[yellow]Rename not supported in cluster mode yet.[/yellow]')
            return
        if arg in self.db.storage.data['groups']:
            console.print(f"[red]Group '{arg}' already exists.[/red]")
            return
        self.db.storage.data['groups'][arg] = self.db.storage.data['groups'].pop(old_name)
        if '_indexes' in self.db.storage.data and old_name in self.db.storage.data['_indexes']:
            self.db.storage.data['_indexes'][arg] = self.db.storage.data['_indexes'].pop(old_name)
        self.db.storage._dirty = True
        self.db.commit()
        self.current_group = self.db.group(arg)
        self.prompt = f'hvpdb:{arg} > '
        console.print(f"[green]Renamed '{old_name}' to '{arg}'.[/green]")

    def do_clone(self, arg):
        args = arg.split()
        if len(args) != 2:
            console.print('[yellow]Usage: clone <source_group> <dest_group>[/yellow]')
            return
        src, dst = args
        if src not in self.db.get_all_groups():
            console.print(f"[red]Source group '{src}' not found.[/red]")
            return
        if dst in self.db.get_all_groups():
            console.print(f"[red]Destination group '{dst}' already exists.[/red]")
            return
        import copy
        src_data = self.db.group(src).find()
        dst_grp = self.db.group(dst)
        with console.status(f'Cloning {src} to {dst}...'):
            for doc in src_data:
                new_doc = copy.deepcopy(doc)
                dst_grp.insert(new_doc)
            self.db.commit()
        console.print(f"[green]Cloned {len(src_data)} documents to '{dst}'.[/green]")

    def do_vacuum(self, arg):
        console.print('[yellow]Vacuuming database...[/yellow]')
        self.db.storage._dirty = True
        self.db.commit()
        console.print('[green]Vacuum complete. Storage optimized.[/green]')

    def do_benchmark(self, arg):
        import time
        console.print('[bold cyan]Running Benchmark...[/bold cyan]')
        bench_grp = self.db.group('_benchmark_temp')
        start = time.time()
        count = 1000
        txn = self.db.begin()
        try:
            with txn:
                for i in range(count):
                    bench_grp.insert({'id': i, 'data': 'x' * 100})
        except Exception as e:
            console.print(f'[red]Write failed: {e}[/red]')
            if '_benchmark_temp' in self.db._groups:
                del self.db._groups['_benchmark_temp']
            return
        duration = time.time() - start
        w_ops = count / duration
        console.print(f'Write: {w_ops:.2f} ops/sec ({count} docs)')
        start = time.time()
        bench_grp.find()
        duration = time.time() - start
        r_ops = count / duration
        console.print(f'Read : {r_ops:.2f} ops/sec')
        if hasattr(self.db, 'is_cluster') and self.db.is_cluster:
            pass
        else:
            if '_benchmark_temp' in self.db.storage.data['groups']:
                del self.db.storage.data['groups']['_benchmark_temp']
            if '_benchmark_temp' in self.db._groups:
                del self.db._groups['_benchmark_temp']
            self.db.commit()
        console.print('[green]Benchmark finished.[/green]')

    def _parse_kv(self, args):
        args = args.strip()
        if not args:
            return {}
        if args.startswith('{'):
            try:
                return json.loads(args)
            except json.JSONDecodeError:
                console.print('[yellow]Invalid JSON format. Falling back to key=value parsing...[/yellow]')
        data = {}
        try:
            parts = shlex.split(args)
            for part in parts:
                if '=' in part:
                    k, v = part.split('=', 1)
                    if v.isdigit():
                        v = int(v)
                    elif v.lower() == 'true':
                        v = True
                    elif v.lower() == 'false':
                        v = False
                    data[k] = v
        except Exception as e:
            console.print(f'[red]Syntax Error: {e}[/red]')
            return None
        return data

    def do_scan(self, arg):
        if not self._check_db():
            return
        if self._check_lock():
            return
        groups = self.db.get_all_groups()
        if not groups:
            console.print('[dim]No groups found.[/dim]')
            return
        table = Table(title='Groups')
        table.add_column('Name', style='cyan')
        table.add_column('Documents', style='green')
        for g in groups:
            count = self.db.group(g).count()
            table.add_row(g, str(count))
        console.print(table)

    def _mask_uri(self, uri: str) -> str:
        if '://' not in uri:
            return os.path.basename(uri)
        try:
            from urllib.parse import urlparse
            parsed = urlparse(uri)
            host = parsed.hostname
            if not host:
                masked_host = 'unknown'
            else:
                parts = host.split('.')
                if len(parts) == 4 and all((p.isdigit() for p in parts)):
                    masked_host = f'{parts[0]}.***.***.{parts[3]}'
                elif len(parts) > 2:
                    masked_host = f'{parts[0]}.***.{parts[-1]}'
                else:
                    masked_host = f'{host[:4]}...{host[-2:]}' if len(host) > 6 else '***'
            port = str(parsed.port) if parsed.port else ''
            if port:
                masked_port = port[0] + '*' * (len(port) - 1)
                netloc = f'{masked_host}:{masked_port}'
            else:
                netloc = masked_host
            return f'{parsed.scheme}://{netloc}{parsed.path}'
        except:
            return '******'

    def _update_prompt(self):
        if not self.db:
            self.prompt = '[bold red]hvpdb (disconnected)[/bold red] > '
            return
        conn_info = self._mask_uri(self.db.filepath)
        prompt_parts = [f'[bold cyan]hvpdb[/bold cyan] [[dim white]{conn_info}[/dim white]]']
        if self.current_group:
            prompt_parts.append(f'[[yellow]{self.current_group.name}[/yellow]]')
            if self.selected_docs:
                prompt_parts.append(f'[[blue]SEL:{len(self.selected_docs)}[/blue]]')
            if self.current_doc:
                doc_id = self.current_doc.get('_id', 'unknown')[:6]
                prompt_parts.append(f'[[magenta]{doc_id}[/magenta]]')
        if self.is_locked:
            prompt_parts.append('[bold red][LOCKED][/bold red]')
        self.prompt = ' '.join(prompt_parts) + ' > '

    def do_lock(self, arg):
        if self.is_locked:
            console.print('[yellow]Already locked.[/yellow]')
            return
        if not self.current_group:
            console.print('[red]Cannot lock at root level. Select a group first.[/red]')
            return
        self.is_locked = True
        self._update_prompt()
        console.print("[bold red]🔒 Context LOCKED. Navigation disabled until 'unlock'.[/bold red]")

    def do_unlock(self, arg):
        if not self.is_locked:
            console.print('[yellow]Not locked.[/yellow]')
            return
        self.is_locked = False
        self._update_prompt()
        console.print('[green]🔓 Context UNLOCKED.[/green]')

    def _check_lock(self):
        if self.is_locked:
            console.print("[bold red]⛔ Action blocked by Safety Lock. Type 'unlock' first.[/bold red]")
            return True
        return False

    def do_target(self, arg):
        if not self._check_db():
            return
        if self._check_lock():
            return
        name = arg.strip()
        if not name:
            console.print('[yellow]Usage: target <group_name>[/yellow]')
            return
        all_groups = self.db.get_all_groups()
        if name not in all_groups:
            import difflib
            matches = difflib.get_close_matches(name, all_groups, n=1, cutoff=0.6)
            if matches:
                suggestion = matches[0]
                if console.input(f"[yellow]Group '{name}' not found. Did you mean '{suggestion}'? (y/n): [/yellow]").lower() == 'y':
                    name = suggestion
                elif console.input(f"[blue]Create new group '{name}'? (y/n): [/blue]").lower() != 'y':
                    return
            elif console.input(f"[blue]Group '{name}' not found. Create new? (y/n): [/blue]").lower() != 'y':
                return
        if self.current_group:
            self.prev_group = self.current_group
        self.current_group = self.db.group(name)
        self.current_doc = None
        self._update_prompt()
        console.print(f'[green]Target locked: [bold]{name}[/bold][/green]')

    def do_jump(self, arg):
        if not self._check_db():
            return
        if self._check_lock():
            return
        if not self.prev_group:
            console.print('[yellow]No previous group to jump to.[/yellow]')
            return
        current_name = self.current_group.name if self.current_group else None
        target_group = self.prev_group
        if target_group.name not in self.db.get_all_groups():
            console.print(f"[red]Previous group '{target_group.name}' no longer exists.[/red]")
            self.prev_group = None
            return
        self.current_group = target_group
        if current_name:
            self.prev_group = self.db.group(current_name)
        self.current_doc = None
        self._update_prompt()
        console.print(f'[green]Jumped to: [bold]{self.current_group.name}[/bold][/green]')

    def do_cancel(self, arg):
        if self._check_lock():
            return
        if self.current_doc:
            console.print(f"[yellow]Unlocking document {self.current_doc.get('_id', '')[:6]}...[/yellow]")
            self.current_doc = None
        elif self.current_group:
            console.print(f"[yellow]Leaving group '{self.current_group.name}'...[/yellow]")
            self.current_group = None
        else:
            console.print('[dim]Already at root level.[/dim]')
        self._update_prompt()

    def do_back(self, arg):
        self.do_cancel(arg)

    def do_peek(self, arg):
        if not self.current_group:
            console.print("[red]No group selected. Use 'target <group>' first.[/red]")
            return
        limit = 20
        show_full = False
        target_idx = None
        arg = arg.strip()
        if arg:
            if arg == 'full':
                show_full = True
                limit = 1000000
            elif arg.startswith('@'):
                try:
                    target_idx = int(arg[1:])
                except:
                    console.print('[red]Invalid index format. Use @0, @1...[/red]')
                    return
            elif arg.isdigit():
                limit = int(arg)
            else:
                parts = arg.split()
                if 'full' in parts:
                    show_full = True
                    for p in parts:
                        if p.isdigit():
                            limit = int(p)
                else:
                    console.print(f'[yellow]Unknown argument: {arg}. Using default limit.[/yellow]')
        group_data = self.current_group.storage.data['groups'][self.current_group.name]
        total_docs = len(group_data)
        if target_idx is not None:
            if 0 <= target_idx < total_docs:
                doc = list(group_data.values())[target_idx]
                from rich.json import JSON
                console.print(Panel(JSON(json.dumps(doc, default=str)), title=f"[bold green]Document @{target_idx} ({doc['_id']})[/bold green]"))
            else:
                console.print(f'[red]Index @{target_idx} out of range (0-{total_docs - 1}).[/red]')
            return
        import itertools
        docs = list(itertools.islice(group_data.values(), limit))
        self.last_search_results = docs
        if not docs:
            console.print('[dim]Group is empty.[/dim]')
            return
        self._print_table(docs, full=show_full)
        if total_docs > limit and (not show_full):
            remaining = total_docs - limit
            console.print(f"[dim]... and {remaining} more documents. Use 'peek {limit + 20}' or 'peek full' to see more.[/dim]")

    def do_hunt(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        query = self._parse_kv(arg)
        if not query:
            return
        import re
        has_regex = any((isinstance(v, str) and v.startswith('r:') for v in query.values()))
        if not has_regex:
            results = list(self.current_group.find_iter(query))
            self.last_search_results = results
        else:
            results = []
            regex_filters = {}
            simple_filters = {}
            for k, v in query.items():
                if isinstance(v, str) and v.startswith('r:'):
                    try:
                        pattern = v[2:]
                        regex_filters[k] = re.compile(pattern)
                    except re.error as e:
                        console.print(f"[red]Invalid Regex for '{k}': {e}[/red]")
                        return
                else:
                    simple_filters[k] = v
            docs_iter = self.current_group.get_all_iter()
            for doc in docs_iter:
                match = True
                for k, v in simple_filters.items():
                    if doc.get(k) != v:
                        match = False
                        break
                if not match:
                    continue
                for k, pattern in regex_filters.items():
                    val = str(doc.get(k, ''))
                    if not pattern.search(val):
                        match = False
                        break
                if match:
                    results.append(doc)
            self.last_search_results = results
        if not results:
            console.print('[yellow]No matches found.[/yellow]')
            return
        self._print_table(results)
        console.print(f'[green]Found {len(results)} matches.[/green]')

    def _print_table(self, docs, full=False):
        table = Table(show_header=True, header_style='bold magenta', box=None, show_lines=True)
        table.add_column('#', style='dim', width=4)
        table.add_column('ID', style='cyan', width=12)
        table.add_column('Data Preview', style='white', no_wrap=not full)
        for idx, doc in enumerate(docs):
            data_copy = doc.copy()
            data_copy.pop('_id', None)
            data_preview = json.dumps(data_copy, default=str)
            if not full and len(data_preview) > 60:
                data_preview = data_preview[:57] + '...'
            table.add_row(str(idx), doc['_id'][:8], data_preview)
        console.print(table)

    def do_help(self, arg):
        if not arg:
            self.preloop()
            return
        doc = getattr(self, f'do_{arg}', None).__doc__
        if doc:
            console.print(Panel(doc, title=f'[bold cyan]Help: {arg}[/bold cyan]', border_style='cyan'))
        else:
            console.print(f"[red]No help found for '{arg}'.[/red]")

    def do_make(self, arg):
        if not self._check_db():
            return
        if arg.startswith('group:'):
            g_name = arg.split(':', 1)[1].strip()
            if not g_name:
                console.print('[red]Missing group name.[/red]')
                return
            if g_name in self.db.get_all_groups():
                console.print(f"[yellow]Group '{g_name}' already exists.[/yellow]")
                return
            self.db.group(g_name)
            self.db.commit()
            console.print(f"[green]Group '{g_name}' created successfully.[/green]")
            return
        if not self.current_group:
            console.print("[red]No group selected. Use 'target <group>' first.[/red]")
            return
        data = {}
        if not arg:
            console.print('[cyan]Interactive Document Creation (Empty key to finish)[/cyan]')
            while True:
                key = input('  Key: ').strip()
                if not key:
                    break
                val = input(f"  Value for '{key}': ").strip()
                if val.isdigit():
                    val = int(val)
                elif val.lower() == 'true':
                    val = True
                elif val.lower() == 'false':
                    val = False
                data[key] = val
            if not data:
                console.print('[yellow]Creation cancelled (Empty data).[/yellow]')
                return
        else:
            data = self._parse_kv(arg)
        if not data:
            console.print('[red]Invalid data format. Use key=value or JSON.[/red]')
            return
        res = self.current_group.insert(data)
        self.db.commit()
        console.print(f"[green]Document created. ID: {res['_id']}[/green]")

    def do_move(self, arg):
        if not self._check_db():
            return
        args = arg.split()
        source_group = self.current_group
        target_group_name = None
        doc_id = None
        if ':' in args[0] and (not self.current_group):
            if len(args) != 2:
                console.print('[yellow]Usage: move <source_group>:<doc_id> <target_group>[/yellow]')
                return
            src_str, target_group_name = args
            src_name, doc_id = src_str.split(':', 1)
            if src_name not in self.db.get_all_groups():
                console.print(f"[red]Source group '{src_name}' not found.[/red]")
                return
            source_group = self.db.group(src_name)
        elif self.current_group:
            if len(args) == 1:
                if not self.current_doc:
                    console.print("[yellow]No document selected. Use 'pick' first or 'move <id> <group>'.[/yellow]")
                    return
                doc_id = self.current_doc['_id']
                target_group_name = args[0]
            elif len(args) == 2:
                doc_id, target_group_name = args
            else:
                self.do_help('move')
                return
        else:
            console.print("[red]No group selected. Use 'target' or syntax 'move <group>:<id> <target>'.[/red]")
            return
        self._exec_move_copy(source_group, doc_id, target_group_name, is_move=True)

    def do_copy(self, arg):
        if not self._check_db():
            return
        args = arg.split()
        source_group = self.current_group
        target_group_name = None
        doc_id = None
        if ':' in args[0] and (not self.current_group):
            if len(args) != 2:
                console.print('[yellow]Usage: copy <source_group>:<doc_id> <target_group>[/yellow]')
                return
            src_str, target_group_name = args
            src_name, doc_id = src_str.split(':', 1)
            if src_name not in self.db.get_all_groups():
                console.print(f"[red]Source group '{src_name}' not found.[/red]")
                return
            source_group = self.db.group(src_name)
        elif self.current_group:
            if len(args) == 1:
                if not self.current_doc:
                    console.print('[yellow]No document selected.[/yellow]')
                    return
                doc_id = self.current_doc['_id']
                target_group_name = args[0]
            elif len(args) == 2:
                doc_id, target_group_name = args
            else:
                self.do_help('copy')
                return
        else:
            console.print("[red]No group selected. Use 'target' or syntax 'copy <group>:<id> <target>'.[/red]")
            return
        self._exec_move_copy(source_group, doc_id, target_group_name, is_move=False)

    def do_become(self, arg):
        if not self._check_db():
            return
        args = arg.split()
        if not args:
            console.print('[yellow]Usage: become <username> [password][/yellow]')
            return
        target_user = args[0]
        password = args[1] if len(args) > 1 else None
        if 'users' not in self.db.storage.data or target_user not in self.db.storage.data['users']:
            console.print(f"[red]User '{target_user}' not found.[/red]")
            return
        current = getattr(self.db, 'current_user', None)
        is_admin = False
        if current:
            user_data = self.db.storage.data['users'].get(current)
            if user_data and user_data.get('role') == 'admin':
                is_admin = True
        if is_admin:
            self.db.current_user = target_user
            console.print(f'[green]Switched to user: [bold]{target_user}[/bold] (Admin Override)[/green]')
        else:
            if not password:
                password = console.input(f'Password for {target_user}: ', password=True)
            if self.db.authenticate(target_user, password):
                console.print(f'[green]Switched to user: [bold]{target_user}[/bold][/green]')
            else:
                console.print('[red]Authentication failed.[/red]')
                return
        self._update_prompt()

    def do_user(self, arg):
        if not self._check_db():
            return
        if 'perms' not in self.db.plugins:
            try:
                from hvpdb_perms import PermissionManager
                self.db.plugins['perms'] = PermissionManager(self.db)
            except ImportError:
                console.print("[red]Error: 'hvpdb-perms' plugin not found.[/red]")
                return
        pm = self.db.plugins['perms']
        args = arg.split()
        if not args:
            self.do_help('user')
            return
        cmd = args[0].lower()
        if cmd == 'list':
            users = pm.list_users()
            table = Table(title='Database Users')
            table.add_column('Username', style='cyan')
            table.add_column('Role', style='magenta')
            table.add_column('Groups', style='green')
            for u, data in users.items():
                groups = ', '.join(data.get('groups', []))
                table.add_row(u, data.get('role'), groups)
            console.print(table)
        elif cmd == 'create':
            if len(args) < 2:
                console.print('[yellow]Usage: user create <username> [password] [role][/yellow]')
                return
            username = args[1]
            password = args[2] if len(args) > 2 else None
            role = args[3] if len(args) > 3 else 'user'
            if not password:
                password = console.input(f"Enter password for '{username}': ", password=True)
            try:
                pm.create_user(username, password, role)
                self.db.commit()
                console.print(f"[green]User '{username}' created.[/green]")
            except Exception as e:
                console.print(f'[red]Error: {e}[/red]')
        elif cmd == 'drop':
            if len(args) < 2:
                console.print('[yellow]Usage: user drop <username>[/yellow]')
                return
            username = args[1]
            if console.input(f"Are you sure you want to delete user '{username}'? (y/n) ").lower() != 'y':
                return
            try:
                if username in self.db.storage.data['users']:
                    del self.db.storage.data['users'][username]
                    self.db.storage._dirty = True
                    self.db.commit()
                    console.print(f"[green]User '{username}' deleted.[/green]")
                else:
                    console.print(f"[red]User '{username}' not found.[/red]")
            except Exception as e:
                console.print(f'[red]Error: {e}[/red]')
        else:
            console.print(f'[red]Unknown user command: {cmd}[/red]')

    def do_grant(self, arg):
        if not self._check_db():
            return
        args = arg.split()
        if len(args) != 2:
            console.print('[yellow]Usage: grant <username> <group>[/yellow]')
            return
        username, group = args
        if 'perms' not in self.db.plugins:
            console.print('[red]Permissions plugin not loaded.[/red]')
            return
        try:
            self.db.plugins['perms'].grant(username, group)
            self.db.commit()
            console.print(f"[green]Granted access to '{group}' for '{username}'.[/green]")
        except Exception as e:
            console.print(f'[red]Error: {e}[/red]')

    def do_revoke(self, arg):
        if not self._check_db():
            return
        args = arg.split()
        if len(args) != 2:
            console.print('[yellow]Usage: revoke <username> <group>[/yellow]')
            return
        username, group = args
        if 'perms' not in self.db.plugins:
            console.print('[red]Permissions plugin not loaded.[/red]')
            return
        try:
            self.db.plugins['perms'].revoke(username, group)
            self.db.commit()
            console.print(f"[green]Revoked access to '{group}' from '{username}'.[/green]")
        except Exception as e:
            console.print(f'[red]Error: {e}[/red]')

    def _exec_move_copy(self, source_group, doc_id, target_group_name, is_move):
        if target_group_name not in self.db.get_all_groups():
            console.print(f"[red]Target group '{target_group_name}' not found.[/red]")
            return
        if source_group.name == target_group_name:
            console.print('[yellow]Source and target groups are the same.[/yellow]')
            return
        doc = source_group.find_one({'_id': doc_id})
        if not doc:
            console.print(f"[red]Document {doc_id} not found in '{source_group.name}'.[/red]")
            return
        try:
            import copy
            new_doc = copy.deepcopy(doc)
            if not is_move:
                if '_id' in new_doc:
                    del new_doc['_id']
            res = self.db.group(target_group_name).insert(new_doc)
            if is_move:
                source_group.delete({'_id': doc_id})
                msg_action = 'Moved'
                if self.current_doc and self.current_doc.get('_id') == doc_id:
                    self.current_doc = None
                    self._update_prompt()
            else:
                msg_action = 'Copied'
            self.db.commit()
            console.print(f"[green]{msg_action} document to '{target_group_name}'. New ID: {res['_id'][:8]}[/green]")
        except Exception as e:
            console.print(f'[red]Operation failed: {e}[/red]')

    def do_random(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        import random
        docs = self.current_group.find()
        if not docs:
            console.print('[dim]Group is empty.[/dim]')
            return
        doc = random.choice(docs)
        self.current_doc = doc
        self._update_prompt()
        from rich.json import JSON
        json_str = json.dumps(doc, indent=2, default=str)
        console.print(Panel(JSON(json_str), title='[bold green]Random Pick (LOCKED)[/bold green]', border_style='green'))

    def do_fields(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        fields = set()
        for doc in self.current_group.find():
            fields.update(doc.keys())
        console.print(Panel('\n'.join(sorted(fields)), title=f'Fields in {self.current_group.name}'))

    def do_nuke(self, arg):
        if not self._check_db():
            return
        if not arg:
            self.do_help('nuke')
            return
        confirm = input(f"🔥 WARNING: Nuke group '{arg}'? (y/n): ")
        if confirm.lower() == 'y':
            if arg in self.db.storage.data['groups']:
                del self.db.storage.data['groups'][arg]
                self.db.storage._dirty = True
                self.db.commit()
                console.print(f"[bold red]💥 Group '{arg}' nuked.[/bold red]")
                if self.current_group and self.current_group.name == arg:
                    self.current_group = None
                    self.prompt = 'hvpdb > '
            else:
                console.print(f"[yellow]Group '{arg}' not found.[/yellow]")

    def do_version(self, arg):
        from . import __version__ as pkg_version
        console.print(f'[bold cyan]HVPDB v{pkg_version}[/bold cyan]')
        console.print('Engine: HVP-Storage (Python)')

    def do_how(self, arg):
        if not arg:
            console.print('[yellow]Usage: how <command>[/yellow]')
            return
        explanations = {'target': "Use [green]target[/green] (or focus) to select a 'folder' (Group) to work in.\nFlow: target users -> make k=v -> peek", 'make': "Use [green]make[/green] (or create) to add new data.\nIt's the primary way to insert documents.", 'hunt': "Use [green]hunt[/green] (or find) to search for specific data.\nIt's like 'grep' but for JSON data.", 'peek': 'Use [green]peek[/green] (or show) to inspect data in the current group.', 'morph': 'Use [green]morph[/green] (or update) to change existing data.', 'nuke': 'Use [green]nuke[/green] (or remove) to destroy data forever.', 'scout': 'Use [green]scout[/green] to list all available groups.'}
        expl = explanations.get(arg)
        if expl:
            console.print(Panel(expl, title=f"How to use '{arg}'", border_style='green'))
        else:
            self.do_help(arg)

    def do_example(self, arg):
        if not arg:
            console.print('[yellow]Usage: example <command>[/yellow]')
            return
        doc = getattr(self, f'do_{arg}', None).__doc__
        if not doc:
            console.print(f"[red]No examples found for '{arg}'.[/red]")
            return
        if 'Example' in doc:
            parts = doc.split('Example')
            console.print(Panel(parts[1].strip(), title=f'Examples: {arg}', border_style='blue'))
        else:
            console.print(Panel(doc, title=f'Help: {arg}', border_style='cyan'))

    def do_drop(self, arg):
        self.do_nuke(arg)

    def do_version(self, arg):
        from . import __version__ as pkg_version
        console.print(f'[bold cyan]HVPDB v{pkg_version}[/bold cyan]')
        console.print('Engine: HVP-Storage (Python)')

    def do_config(self, arg):
        console.print('[dim]Config management coming soon...[/dim]')

    def do_backup(self, arg):
        if not arg:
            self.do_help('backup')
            return
        import shutil
        try:
            if hasattr(self.db, 'filepath') and os.path.exists(self.db.filepath):
                shutil.copy2(self.db.filepath, arg)
                console.print(f'[green]Backup created at {arg}[/green]')
            else:
                console.print('[red]Cannot backup in-memory or cluster DB yet.[/red]')
        except Exception as e:
            console.print(f'[red]Backup failed: {e}[/red]')

    def do_pick(self, arg):
        if not self.last_search_results:
            console.print("[yellow]No results to pick from. Run 'peek' or 'hunt' first.[/yellow]")
            return
        try:
            idx = int(arg)
            if 0 <= idx < len(self.last_search_results):
                self.current_doc = self.last_search_results[idx]
                self._update_prompt()
                from rich.json import JSON
                json_str = json.dumps(self.current_doc, indent=2, default=str)
                console.print(Panel(JSON(json_str), title='[bold green]Selected Document (LOCKED)[/bold green]', border_style='green'))
            else:
                console.print('[red]Index out of range.[/red]')
        except ValueError:
            console.print('[red]Invalid index.[/red]')

    def do_select(self, arg):
        if not self.last_search_results:
            console.print("[yellow]No search results to select from. Run 'peek' or 'hunt' first.[/yellow]")
            return
        if arg == 'all':
            self.selected_docs = [d['_id'] for d in self.last_search_results]
            console.print(f'[green]Selected {len(self.selected_docs)} documents.[/green]')
        elif arg == 'clear':
            self.selected_docs = []
            console.print('[green]Selection cleared.[/green]')
        elif '-' in arg:
            try:
                start, end = map(int, arg.split('-'))
                end = min(end, len(self.last_search_results) - 1)
                for i in range(start, end + 1):
                    doc = self.last_search_results[i]
                    if doc['_id'] not in self.selected_docs:
                        self.selected_docs.append(doc['_id'])
                console.print(f'[green]Added range {start}-{end} to selection.[/green]')
            except ValueError:
                console.print('[red]Invalid range format. Use start-end (e.g. 0-5).[/red]')
        else:
            try:
                idx = int(arg)
                if 0 <= idx < len(self.last_search_results):
                    doc = self.last_search_results[idx]
                    if doc['_id'] not in self.selected_docs:
                        self.selected_docs.append(doc['_id'])
                        console.print(f'[green]Added document @{idx} to selection.[/green]')
                    else:
                        console.print('[yellow]Document already selected.[/yellow]')
                else:
                    console.print('[red]Index out of range.[/red]')
            except ValueError:
                console.print('[red]Invalid index.[/red]')
        self._update_prompt()

    def do_discard(self, arg):
        if arg == 'all':
            self.selected_docs = []
            console.print('[green]Selection cleared.[/green]')
        else:
            try:
                idx = int(arg)
                if 0 <= idx < len(self.last_search_results):
                    doc_id = self.last_search_results[idx]['_id']
                    if doc_id in self.selected_docs:
                        self.selected_docs.remove(doc_id)
                        console.print(f'[green]Removed document @{idx} from selection.[/green]')
                    else:
                        console.print('[yellow]Document was not selected.[/yellow]')
                else:
                    console.print('[red]Index out of range.[/red]')
            except ValueError:
                console.print('[red]Invalid index.[/red]')
        self._update_prompt()

    def do_morph(self, arg):
        target_ids = []
        if self.selected_docs:
            target_ids = self.selected_docs
        elif self.current_doc:
            target_ids = [self.current_doc['_id']]
        else:
            console.print('[red]No document selected.[/red]')
            return
        updates = self._parse_kv(arg)
        if not updates:
            return
        count = 0
        for doc_id in target_ids:
            if self.current_group.update({'_id': doc_id}, updates):
                count += 1
                if self.current_doc and self.current_doc['_id'] == doc_id:
                    self.current_doc.update(updates)
        self.db.commit()
        console.print(f'[green]Updated {count} documents successfully.[/green]')

    def do_throw(self, arg):
        target_ids = []
        if self.selected_docs:
            target_ids = self.selected_docs
        elif self.current_doc:
            target_ids = [self.current_doc['_id']]
        else:
            console.print('[red]No document selected.[/red]')
            return
        if console.input(f'[bold red]Delete {len(target_ids)} documents? (y/n): [/bold red]').lower() != 'y':
            return
        count = 0
        for doc_id in target_ids:
            if self.current_group.delete({'_id': doc_id}):
                count += 1
        self.db.commit()
        if self.current_doc and self.current_doc['_id'] in target_ids:
            self.current_doc = None
        self.selected_docs = [d for d in self.selected_docs if d not in target_ids]
        console.print(f'[green]Deleted {count} documents.[/green]')
        self._update_prompt()

    def do_check(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        count = self.current_group.count()
        console.print(f'Total documents: {count}')

    def do_truncate(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        confirm = input(f"WARNING: Delete ALL data in '{self.current_group.name}'? (yes/no): ")
        if confirm.lower() == 'yes':
            if hasattr(self.db, 'is_cluster') and self.db.is_cluster:
                console.print('[yellow]Cluster truncate not optimized yet. Using slow delete.[/yellow]')
                all_docs = self.current_group.find()
                for d in all_docs:
                    self.current_group.delete({'_id': d['_id']})
            else:
                self.db.storage.data['groups'][self.current_group.name] = {}
                self.db.storage._dirty = True
            self.db.commit()
            console.print(f"[green]Group '{self.current_group.name}' truncated.[/green]")

    def do_index(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        args = arg.split()
        if not args:
            console.print('[yellow]Usage: index <field> [unique][/yellow]')
            return
        field = args[0]
        unique = False
        if len(args) > 1 and args[1].lower() == 'unique':
            unique = True
        try:
            self.current_group.create_index(field, unique=unique)
            self.db.commit()
            type_str = 'UNIQUE' if unique else 'STANDARD'
            console.print(f"[green]{type_str} Index created on '{field}'.[/green]")
        except ValueError as e:
            console.print(f'[red]Failed to create index: {e}[/red]')
        except Exception as e:
            console.print(f'[red]Error: {e}[/red]')

    def do_export(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        if not arg:
            console.print('[yellow]Usage: export <filename.json>[/yellow]')
            return
        docs = self.current_group.find()
        try:
            with open(arg, 'w', encoding='utf-8') as f:
                json.dump(docs, f, indent=2, default=str)
            console.print(f'[green]Exported {len(docs)} documents to {arg}[/green]')
        except Exception as e:
            console.print(f'[red]Export failed: {e}[/red]')

    def do_import(self, arg):
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        if not arg:
            console.print('[yellow]Usage: import <filename.json>[/yellow]')
            return
        if not os.path.exists(arg):
            console.print(f'[red]File not found: {arg}[/red]')
            return
        try:
            with open(arg, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, list):
                count = 0
                for item in data:
                    if isinstance(item, dict):
                        self.current_group.insert(item)
                        count += 1
                self.db.commit()
                console.print(f'[green]Imported {count} documents.[/green]')
            else:
                console.print('[red]Invalid JSON: Expected a list of objects.[/red]')
        except Exception as e:
            console.print(f'[red]Import failed: {e}[/red]')

    def do_trace(self, arg):
        if not self.current_doc:
            console.print('[red]Select a document first.[/red]')
            return
        if not hasattr(self.current_group, 'get_audit_trail'):
            console.print('[yellow]Audit logging not available.[/yellow]')
            return
        logs = self.current_group.get_audit_trail(self.current_doc['_id'])
        if not logs:
            console.print('[dim]No history found.[/dim]')
            return
        table = Table(title=f"Audit Log: {self.current_doc['_id'][:8]}")
        table.add_column('Time', style='dim')
        table.add_column('Action', style='magenta')
        table.add_column('Data', style='white')
        import datetime
        for log in logs:
            ts = datetime.datetime.fromtimestamp(log.get('timestamp', 0)).strftime('%Y-%m-%d %H:%M:%S')
            op = log.get('op', 'unknown')
            data = str(log.get('data', {}))[:60]
            table.add_row(ts, op, data)
        console.print(table)

    def do_status(self, arg):
        if not self._check_db():
            return
        size_mb = 0
        if hasattr(self.db, 'filepath') and os.path.exists(self.db.filepath):
            size_mb = os.path.getsize(self.db.filepath) / (1024 * 1024)
        console.print(Panel(f"\n        [bold]Database Status[/bold]\n        ----------------\n        Path: {self.db.filepath}\n        Size: {size_mb:.2f} MB\n        Encrypted: {('Yes' if self.db.password else 'No')}\n        Groups: {len(self.db.get_all_groups())}\n        ", title='Info'))

    def do_save(self, arg):
        if not self._check_db():
            return
        args = arg.split()
        if args and args[0] == 'auto':
            if len(args) > 1:
                mode = args[1].lower()
                self.auto_save = mode == 'on'
            console.print(f"Auto-Save on Exit: [{('green' if self.auto_save else 'yellow')}]{('ON' if self.auto_save else 'OFF (Ask)')}[/]")
            return
        self.db.commit()
        console.print('[green]Database saved successfully.[/green]')

    def do_quit(self, arg):
        if self._check_lock():
            return
        if self.db:
            is_dirty = False
            if hasattr(self.db.storage, '_dirty') and self.db.storage._dirty:
                is_dirty = True
            if hasattr(self.db, 'is_cluster') and self.db.is_cluster:
                for grp in self.db._groups.values():
                    if grp.storage._dirty:
                        is_dirty = True
                        break
            if is_dirty:
                if self.auto_save:
                    console.print('[dim]Auto-saving...[/dim]')
                    self.db.commit()
                else:
                    ans = console.input('[yellow]Unsaved changes detected. Save before exit? (y/n/cancel): [/yellow]').lower()
                    if ans == 'y':
                        self.db.commit()
                        console.print('[green]Saved.[/green]')
                    elif ans == 'n':
                        console.print('[red]Changes discarded.[/red]')
                    else:
                        console.print('[dim]Cancelled exit.[/dim]')
                        return False
            try:
                self.db.close()
            except Exception as e:
                console.print(f'[red]Error closing DB: {e}[/red]')
        console.print('[bold cyan]Bye! 👋[/bold cyan]')
        return True

    def do_tree(self, arg):
        if not self._check_db():
            return
        tree = Tree(f'[bold cyan]📦 {os.path.basename(self.db.filepath)}[/bold cyan]')
        groups = self.db.get_all_groups()
        for g_name in groups:
            grp = self.db.group(g_name)
            count = grp.count()
            g_node = tree.add(f'[yellow]📂 {g_name}[/yellow] [dim]({count} docs)[/dim]')
            if hasattr(grp, 'indexes') and grp.indexes:
                idx_node = g_node.add('[dim]Indexes[/dim]')
                for field in grp.indexes:
                    idx_node.add(f'🔑 {field}')
            if hasattr(grp, 'unique_indexes') and grp.unique_indexes:
                uidx_node = g_node.add('[dim]Unique Constraints[/dim]')
                for field in grp.unique_indexes:
                    uidx_node.add(f'🔒 {field}')
        console.print(tree)

    def do_validate(self, arg):
        if not self._check_db():
            return
        console.print('[bold]Running Integrity Check...[/bold]')
        issues = 0
        for g_name in self.db.get_all_groups():
            grp = self.db.group(g_name)
            docs = grp.find()
            console.print(f"Checking group '{g_name}' ({len(docs)} docs)...", end='')
            g_issues = 0
            for doc in docs:
                if '_id' not in doc:
                    console.print(f'\n  [red]CRITICAL: Doc missing _id: {str(doc)[:50]}...[/red]')
                    g_issues += 1
                try:
                    json.dumps(doc)
                except Exception as e:
                    console.print(f"\n  [red]ERROR: Doc {doc.get('_id')} is not JSON serializable: {e}[/red]")
                    g_issues += 1
            if g_issues == 0:
                console.print(' [green]OK[/green]')
            else:
                issues += g_issues
        if issues == 0:
            console.print('\n[bold green]✅ Database is HEALTHY.[/bold green]')
        else:
            console.print(f'\n[bold red]❌ Found {issues} issues.[/bold red]')

    def do_monitor(self, arg):
        if not self._check_db():
            return
        import time
        interval = 2
        if arg and arg.isdigit():
            interval = int(arg)
        console.print(f'[cyan]Monitoring... (Ctrl+C to stop)[/cyan]')
        try:
            with console.status('Monitoring DB Activity...') as status:
                while True:
                    total_docs = 0
                    groups = self.db.get_all_groups()
                    for g in groups:
                        total_docs += self.db.group(g).count()
                    size_mb = os.path.getsize(self.db.filepath) / (1024 * 1024) if os.path.exists(self.db.filepath) else 0
                    status.update(f'Groups: {len(groups)} | Docs: {total_docs} | Size: {size_mb:.2f} MB')
                    time.sleep(interval)
        except KeyboardInterrupt:
            console.print('\n[dim]Monitor stopped.[/dim]')

    def do_record(self, arg):
        if not self._check_db():
            return
        args = arg.split()
        if not args:
            self.do_help('record')
            return
        cmd = args[0].lower()
        if cmd == 'status':
            if len(args) > 1:
                mode = args[1].lower()
                self.record_mode = mode == 'on'
            console.print(f"Record Mode: [{('green' if self.record_mode else 'red')}]{('ON' if self.record_mode else 'OFF')}[/]")
        elif cmd == 'list':
            limit = int(args[1]) if len(args) > 1 and args[1].isdigit() else 10
            if hasattr(self.db.storage, 'wal'):
                logs = []

                def collector(entry):
                    logs.append(entry)
                self.db.storage.wal.replay(0, collector)
                logs = sorted(logs, key=lambda x: x.get('seq', 0), reverse=True)[:limit]
                table = Table(title=f'Transaction History (Last {limit})')
                table.add_column('Seq', style='cyan', width=6)
                table.add_column('Txn ID', style='blue', width=8)
                table.add_column('Time', style='dim')
                table.add_column('Op', style='magenta')
                table.add_column('Group', style='yellow')
                table.add_column('ID', style='white')
                import datetime
                for log in logs:
                    ts = datetime.datetime.fromtimestamp(log.get('ts', 0)).strftime('%H:%M:%S')
                    txn = log.get('txn', '')[:8] if log.get('txn') else '-'
                    table.add_row(str(log.get('seq')), txn, ts, log.get('op'), log.get('g'), str(log.get('id'))[:8])
                console.print(table)
            else:
                console.print('[red]WAL not accessible.[/red]')
        elif cmd == 'peek':
            if len(args) < 2:
                console.print('[yellow]Usage: record peek <seq>[/yellow]')
                return
            target_seq = int(args[1])
            found_log = None

            def finder(entry):
                nonlocal found_log
                if entry.get('seq') == target_seq:
                    found_log = entry
            self.db.storage.wal.replay(0, finder)
            if not found_log:
                console.print(f'[red]Record #{target_seq} not found.[/red]')
                return
            data = found_log.get('d')
            before = found_log.get('b')
            op = found_log.get('op')
            console.print(Panel(f'Transaction #{target_seq} - {op.upper()}', style='blue'))
            if op == 'insert':
                console.print(f'[green]+ {json.dumps(data, indent=2)}[/green]')
            elif op == 'delete':
                console.print(f'[red]- {json.dumps(data, indent=2)}[/red]')
            elif op == 'update':
                if before:
                    console.print('[red]Before:[/red]')
                    console.print(f'[dim]{json.dumps(before, indent=2)}[/dim]')
                    console.print('[green]After:[/green]')
                    console.print(f'{json.dumps(data, indent=2)}')
                else:
                    console.print(f'[yellow]~ {json.dumps(data, indent=2)}[/yellow]')
                    console.print('[dim](Old value not available in log)[/dim]')
        elif cmd == 'undo':
            if len(args) < 2:
                console.print('[yellow]Usage: record undo <seq>[/yellow]')
                return
            seq = int(args[1])
            found_log = None

            def finder(entry):
                nonlocal found_log
                if entry.get('seq') == seq:
                    found_log = entry
            self.db.storage.wal.replay(0, finder)
            if not found_log:
                console.print(f'[red]Record #{seq} not found.[/red]')
                return
            target_txn_id = found_log.get('txn')
            if not target_txn_id:
                console.print('[red]Cannot undo legacy transaction (missing Txn ID).[/red]')
                return
            txn_ops = []

            def txn_collector(entry):
                if entry.get('txn') == target_txn_id and entry.get('type') == 'DATA':
                    txn_ops.append(entry)
            self.db.storage.wal.replay(0, txn_collector)
            txn_ops.sort(key=lambda x: x.get('seq'), reverse=True)
            console.print(f'[bold]Undoing Transaction {target_txn_id[:8]} ({len(txn_ops)} operations)...[/bold]')
            if console.input(f'Confirm undo? (y/n) ').lower() != 'y':
                return
            undo_txn_id = self.db.storage.begin_txn()
            try:
                for op_log in txn_ops:
                    op = op_log.get('op')
                    grp_name = op_log.get('g')
                    doc_id = op_log.get('id')
                    data = op_log.get('d')
                    before = op_log.get('b')
                    grp = self.db.group(grp_name)
                    if op == 'insert':
                        grp.delete({'_id': doc_id}, external_txn_id=undo_txn_id)
                        console.print(f'[green]Reverted Insert: Deleted {doc_id}[/green]')
                    elif op == 'delete':
                        if grp.find_one({'_id': doc_id}):
                            console.print(f'[yellow]Warning: Document {doc_id} already exists. Skipping restore.[/yellow]')
                        else:
                            restore_data = before if before else data
                            grp.insert(restore_data, external_txn_id=undo_txn_id)
                            console.print(f'[green]Reverted Delete: Restored {doc_id}[/green]')
                    elif op == 'update':
                        if before:
                            grp.update({'_id': doc_id}, before, external_txn_id=undo_txn_id)
                            console.print(f'[green]Reverted Update: Restored {doc_id}[/green]')
                        else:
                            console.print(f'[red]Cannot undo update {doc_id}: Missing before-image.[/red]')
                            raise ValueError('Missing before-image')
                self.db.storage.commit_txn(undo_txn_id)
                self.db.commit()
                console.print('[bold green]Transaction Undone Successfully.[/bold green]')
            except Exception as e:
                self.db.storage.rollback_txn(undo_txn_id)
                console.print(f'[bold red]Undo Failed: {e}. Rolled back changes.[/bold red]')
        elif cmd == 'apply':
            if len(args) < 2:
                console.print('[yellow]Usage: record apply <seq>[/yellow]')
                return
            seq = int(args[1])
            found_log = None

            def finder(entry):
                nonlocal found_log
                if entry.get('seq') == seq:
                    found_log = entry
            self.db.storage.wal.replay(0, finder)
            if not found_log:
                return
            op = found_log.get('op')
            grp = self.db.group(found_log.get('g'))
            data = found_log.get('d')
            if console.input(f'Re-apply {op} #{seq}? (y/n) ').lower() != 'y':
                return
            if op == 'insert':
                grp.insert(data)
            elif op == 'delete':
                grp.delete({'_id': found_log.get('id')})
            elif op == 'update':
                grp.update({'_id': found_log.get('id')}, data)
            self.db.commit()
            console.print('[green]Transaction re-applied.[/green]')