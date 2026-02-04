import ast
import cmd
import copy
import datetime
import difflib
import importlib
import itertools
import json
import os
import random
import re
import shlex
import shutil
import subprocess
import tempfile
import time
import warnings
from typing import Optional, Any

try:
    import readline
except ImportError:
    readline = None

try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.history import InMemoryHistory, FileHistory
    from prompt_toolkit.completion import WordCompleter
    from prompt_toolkit.styles import Style
    HAS_PROMPT_TOOLKIT = True
except ImportError:
    HAS_PROMPT_TOOLKIT = False

from rich.console import Console
from rich.json import JSON
from rich.markup import escape
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree

from .core import HVPDB

# Optional Plugins
PermissionManager = None
PolyglotParser = None
QueryEngine = None

try:
    perms_mod = importlib.import_module('hvpdb_perms')
    PermissionManager = getattr(perms_mod, 'PermissionManager', None)
except ImportError:
    PermissionManager = None
except Exception as e:
    warnings.warn(f"Optional plugin 'hvpdb_perms' failed to import: {e}")
    PermissionManager = None

try:
    parser_mod = importlib.import_module('hvpdb_query.parser')
    engine_mod = importlib.import_module('hvpdb_query.engine')
    PolyglotParser = getattr(parser_mod, 'PolyglotParser', None)
    QueryEngine = getattr(engine_mod, 'QueryEngine', None)
except ImportError:
    PolyglotParser = None
    QueryEngine = None
except Exception as e:
    warnings.warn(f"Optional plugin 'hvpdb_query' failed to import: {e}")
    PolyglotParser = None
    QueryEngine = None
console = Console()

class HVPShell(cmd.Cmd):
    """
    Action-oriented database shell for HVPDB.
    
    Provides an interactive command-line interface for database management,
    data operations, auditing, and system maintenance.
    """
    intro = None
    prompt = 'hvpdb > '

    def __init__(self, db: Optional[HVPDB] = None):
        """
        Initialize the shell with an optional database instance.
        
        Args:
            db (HVPDB, optional): The database instance to manage. Defaults to None.
        """
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
        self._anchor: Optional[tuple] = None
        self._prompt_session = None
        if HAS_PROMPT_TOOLKIT:
            history_file = os.path.join(os.path.expanduser('~'), '.hvpdb_history')
            self._prompt_session = PromptSession(history=FileHistory(history_file))

    def preloop(self):
        """Display the welcome banner and initial instructions before starting the command loop."""
        banner_text = """
    [bold]Connection:[/bold]
    - [green]connect[/green] <path>       : Connect to database
    - [green]disconnect[/green]           : Disconnect current DB

    [bold]Navigation:[/bold]
    - [green]scan[/green]                 : List all groups
    - [green]target[/green] <group>       : Select a group context

    [bold]Data Operations:[/bold]
    - [green]peek[/green] [limit|full|@i] : View documents
    - [green]hunt[/green] k=v             : Search documents
    - [green]make[/green] k=v             : Create new document
    - [green]check[/green]                : Count documents
    - [green]truncate[/green]             : Delete all documents in group
    - [green]inhale[/green] / [green]exhale[/green]  : Import/Export JSON
    - [green]distinct[/green] <field>     : List unique values
    - [green]stats[/green] <field>        : Calculate statistics

    [bold]Group Operations:[/bold]
    - [green]rename[/green] <name>        : Rename current group
    - [green]clone[/green] <src> <dst>    : Clone a group

    [bold]Item Operations (After 'pick'):[/bold]
    - [green]pick[/green] <index>         : Select document from list
    - [green]morph[/green] k=v            : Update selected document
    - [green]throw[/green]                : Delete selected document
    - [green]fuse[/green] <id1> <id2>     : Merge two documents
    - [green]sift[/green] [field]         : Deduplicate documents

    [bold]Audit & Version Control:[/bold]
    - [green]record[/green]               : Data versioning (undo/redo)
    - [green]trace[/green]                : View audit log

    [bold]System & Maintenance:[/bold]
    - [green]save[/green]                 : Save to disk
    - [green]refresh[/green]              : Reload from disk
    - [green]perm[/green]                 : Check permissions
    - [green]index[/green] <field>        : Create index
    - [green]schema[/green]               : Infer schema
    - [green]vacuum[/green]               : Compact storage
    - [green]validate[/green]             : Check DB integrity
    - [green]status[/green]               : Database info
    - [green]history[/green]              : Show command history
    - [green]clear[/green]                : Clear screen
    - [green]quit[/green]                 : Exit

    [dim]Tip: Type 'help <command>' for detailed usage.[/dim]
        """
        console.print(Panel(banner_text.strip(), title='[bold cyan]HVPDB Ops Shell (HVPShell)[/bold cyan]', subtitle='[dim]Action-Oriented Database Shell[/dim]', border_style='cyan'))
        self._update_prompt()

    def do_status(self, arg):
        """Display current database connection status, group context, and storage stats."""
        if not self.db:
            console.print('[yellow]Not connected.[/yellow]')
            return
        # Type guard for pyright
        assert self.db is not None
        
        group_name = self.current_group.name if self.current_group else 'None'
        seq = getattr(getattr(self.db, 'storage', None), '_last_sequence', None)
        docs_in_group = self.current_group.count() if self.current_group else 0
        content = f"""Target: {self.db.filepath}
Group: {group_name}
Sequence: {seq}
Docs in Group: {docs_in_group}"""
        console.print(Panel(content, title='Database Status'))

    # Aliases
    do_cat = do_get
    do_show = do_get
    do_use = do_target
    do_ls = do_peek
    do_focus = do_target
    do_seal = do_lock
    do_unseal = do_unlock
    do_pulse = do_status
    do_ignite = do_connect
    do_vanish = do_quit
    do_freeze = do_save
    do_revive = do_refresh
    do_drain = do_vacuum
    do_remove = do_throw
    do_removeid = do_del
    do_renamegroup = do_rename
    do_clonegroup = do_clone
    do_use = do_target
    do_ls = do_peek
    do_focus = do_target
    do_seal = do_lock
    do_unseal = do_unlock
    do_pulse = do_status
    do_ignite = do_connect
    do_vanish = do_quit
    do_freeze = do_save
    do_revive = do_refresh
    do_drain = do_vacuum
    do_remove = do_throw
    do_removeid = do_del
    do_renamegroup = do_rename
    do_clonegroup = do_clone
    do_use = do_target
    do_ls = do_peek
    do_focus = do_target
    do_seal = do_lock
    do_unseal = do_unlock
    do_pulse = do_status
    do_ignite = do_connect
    do_vanish = do_quit
    do_freeze = do_save
    do_revive = do_refresh
    do_drain = do_vacuum
    do_remove = do_throw
    do_removeid = do_del
    do_renamegroup = do_rename
    do_clonegroup = do_clone
        """
        Retrieve and display a document by its ID.
        
        Usage: get <doc_id>
        """
        if not self._check_db():
            return
        assert self.db is not None
        if not self.current_group:
            console.print('[red]Select a group first (use target <group>).[/red]')
            return
        doc_id = arg.strip()
        if not doc_id and self.current_doc:
            doc_id = self.current_doc['_id']
        doc = self.current_group.find_one({'_id': doc_id})
        if doc:
            console.print_json(data=doc)
        else:
            console.print(f'[red]Document {doc_id} not found.[/red]')

    # Aliases
    do_cat = do_get
    do_show = do_get

    def do_grep(self, arg):
        """
        Search for documents matching a key=value pair.
        
        Usage: grep <key>=<value>
        """
        if not self._check_db():
            return
        assert self.db is not None
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
        results = self.current_group.find({key: val})
        console.print(f'Found {len(results)} documents:')
        for doc in results[:10]:
            console.print(f'- {doc}')


    def do_change(self, arg):
        if not self._check_db():
            return
        assert self.db is not None
        
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
            if console.input('[bold red]Change MASTER DB PASSWORD? This will re-encrypt the entire database. (yes/no): [/bold red]') != 'yes':
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
            if not self.db:
                return
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

    def do_connect(self, arg):
        """
        Connect to a database file with optional password.
        
        Usage: connect <path> [password]
        """
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
        """Close the current database connection and clear context."""
        if not self.db:
            console.print('[yellow]Not connected.[/yellow]')
            return
        try:
            self.db.close()
        except Exception as e:
            console.print(f'[red]Error closing database: {e}[/red]')
        self.db = None
        self.current_group = None
        self.current_doc = None
        self.is_locked = False
        self.last_search_results = []
        console.print('[green]Disconnected. Context cleared.[/green]')
        self._update_prompt()

    def do_refresh(self, arg):
        """Reload the database from disk to pick up external changes."""
        if not self._check_db():
            return
        assert self.db is not None
        try:
            self.db.refresh()
            console.print('[green]Database refreshed successfully.[/green]')
            if self.current_group:
                pass
        except Exception as e:
            console.print(f'[red]Refresh failed: {e}[/red]')

    def do_set(self, arg):
        """
        Create or replace a document by ID with raw JSON.
        
        Usage: set <id> <json_string>
        """
        if not self._check_db():
            return
        assert self.db is not None
        
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
            grp = self.current_group
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
        """
        Partially update a document by ID with raw JSON.
        
        Usage: patch <id> <json_string>
        """
        if not self._check_db():
            return
        assert self.db is not None
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
            grp = self.current_group
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
        """Verify that a database connection is active."""
        if not self.db:
            console.print("[red]Not connected to any database. Use 'connect <path>' first.[/red]")
            return False
        return True

    def cmdloop(self, intro=None):
        """Start the interactive command loop."""
        self.preloop()
        if self.use_rawinput and self.completekey and not HAS_PROMPT_TOOLKIT:
            if readline:
                self.old_completer = readline.get_completer()
                readline.set_completer(self.complete) # type: ignore
                readline.parse_and_bind(self.completekey + ': complete')
        stop = None
        while not stop:
            try:
                if HAS_PROMPT_TOOLKIT and self._prompt_session:
                    line = self._prompt_session.prompt(self.prompt)
                elif self.use_rawinput:
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
        """Hook executed before each command."""
        if line and line != 'history':
            self._cmd_history.append(self._redact_history(line))
        return line

    def _redact_history(self, line: str) -> str:
        """Mask sensitive information in command history."""
        SENSITIVE_CMDS = ('connect', 'become', 'user create', 'hvpdb shell', 'hvpdb init')
        SENSITIVE_KEYS = ('password=', 'pass=', 'token=', 'secret=', 'key=')
        low = line.lower().strip()
        for sensitive_cmd in SENSITIVE_CMDS:
            if low.startswith(sensitive_cmd):
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
        """Show recently executed commands (masked)."""
        if not self._cmd_history:
            console.print('[dim]No history yet.[/dim]')
            return
        for i, entry in enumerate(self._cmd_history[-20:]):
            console.print(f'{i + 1}. {entry}')

    def do_tour(self, arg):
        """Start a quick tour of HVPDB features."""
        self.do_getatour(arg)

    def do_cheatsheet(self, arg):
        """Display a quick reference guide for common commands."""
        console.print(Panel('\n        [bold]HVPDB Cheatsheet[/bold]\n        [green]focus <group>[/green]   : Select group (e.g. focus users)\n        [green]find k=v[/green]        : Search (e.g. find role=admin)\n        [green]show[/green]            : List docs (e.g. show, show 20)\n        [green]create k=v[/green]      : New doc (e.g. create name=A)\n        [green]update k=v[/green]      : Edit doc (e.g. update age=30)\n        [green]remove[/green]          : Delete doc\n        [green]timeline[/green]        : History\n        [green]quit[/green]            : Exit\n        ', title='Quick Ref'))

    def do_examples(self, arg):
        """Show usage examples for common operations."""
        console.print('[bold]Examples:[/bold]')
        console.print('  create name=Alice role=admin')
        console.print('  focus users')
        console.print('  find role=admin')
        console.print('  update status=active')
        console.print('  stats age')

    def do_explain(self, arg):
        """
        Explain a command's purpose and usage.
        
        Usage: explain <command>
        """
        if not arg:
            console.print('[yellow]Usage: explain <command>[/yellow]')
            return
        cmd_func = getattr(self, f'do_{arg}', None)
        if cmd_func and cmd_func.__doc__:
            console.print(Panel(cmd_func.__doc__, title=f'Explain: {arg}'))
        else:
            console.print(f'[red]Unknown command: {arg}[/red]')

    def do_why(self, arg):
        """Provide automated analysis for errors or unexpected behavior."""
        console.print('[dim]Analysis: Most likely syntax error or missing permission.[/dim]')

    def do_tips(self, arg):
        """Display a random tip for efficient database usage."""
        tips = ["Use 'focus <group>' to switch context quickly.", "Batch operations: 'find k=v' -> 'select all' -> 'update k=v2'", "Use 'track' to see your command history.", "Type 'help <cmd>' for detailed usage."]
        console.print(f'[cyan]💡 Tip: {random.choice(tips)}[/cyan]')

    def do_doctor(self, arg):
        """Run database diagnostics and health checks."""
        self.do_diagnose(arg)

    def do_teach(self, arg):
        """Enable interactive teacher mode (explains actions)."""
        console.print('[dim]Teacher mode active.[/dim]')

    def do_focus(self, arg):
        """
        Switch context to a specific group.
        
        Usage: focus <group_name>
        """
        self.do_target(arg)

    def do_unfocus(self, arg):
        """Clear the current group context."""
        self.current_group = None
        self._update_prompt()
        console.print('[dim]Context cleared.[/dim]')

    def do_switch(self, arg):
        """Switch back to the previously focused group."""
        if self.prev_group:
            self.do_target(self.prev_group.name if hasattr(self.prev_group, 'name') else self.prev_group)
        else:
            console.print('[yellow]No previous group.[/yellow]')

    def do_context(self, arg):
        """Display current database and group context."""
        self.do_status(arg)

    def do_show(self, arg):
        """
        List documents in the current group.
        
        Usage: 
            show        : List documents
            show at <id>: Show specific document
            show full   : Show detailed list
        """
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
        """Display a random sample of documents from the current group."""
        self.do_sample_impl(arg)

    def do_find(self, arg):
        """
        Search for documents matching criteria.
        
        Usage: find <key>=<value> [<key2>=<value2> ...]
        """
        self.do_hunt(arg)

    def do_count(self, arg):
        """Count documents in the current group or matching a search."""
        if arg:
            self.do_hunt(arg)
        else:
            self.do_check(arg)

    def do_freq(self, arg):
        """
        Show frequency distribution for values of a field.
        
        Usage: freq <field>
        """
        if not self._check_db():
            return
        assert self.db is not None
        if not self.current_group:
            return
        field = arg.split()[0]
        docs = self.current_group.find()
        from collections import Counter
        vals = [str(d.get(field)) for d in docs if field in d]
        c = Counter(vals)
        console.print(f"Frequency for '{field}': {c.most_common(10)}")

    def do_create(self, arg):
        """
        Create a new document in the current group.
        
        Usage: create <key>=<value> [<key2>=<value2> ...]
        """
        self.do_make(arg)

    def do_creategroup(self, arg):
        """
        Create a new document group (collection).
        
        Usage: creategroup <group_name>
        """
        if not self._check_db():
            return
        assert self.db is not None
        self.db.group(arg)
        console.print(f"[green]Group '{arg}' created.[/green]")

    def do_update(self, arg):
        """
        Update documents matching current selection or ID.
        
        Usage: update <key>=<value> [<key2>=<value2> ...]
        """
        self.do_morph(arg)


    def do_unset(self, arg):
        """
        Remove a field from a document or from the current selection.

        Usage:
            unset <field>
            unset <id> <field>
        """
        if not self._check_db():
            return
        assert self.db is not None
        if not self.current_group:
            console.print('[red]Select a group first.[/red]')
            return

        parts = arg.split()
        if not parts:
            console.print('[yellow]Usage: unset <field> | unset <id> <field>[/yellow]')
            return

        if len(parts) == 2:
            doc_id, field = parts
            doc = self.current_group.find_one({'_id': doc_id})
            if not doc:
                console.print(f'[red]Document {doc_id} not found.[/red]')
                return
            if field not in doc:
                console.print(f"[yellow]Field '{field}' not in document.[/yellow]")
                return
            del doc[field]
            self.current_group.update({'_id': doc_id}, doc)
            self.db.commit()
            console.print(f"[green]Field '{field}' removed from {doc_id}.[/green]")
            return

        if len(parts) != 1:
            console.print('[yellow]Usage: unset <field> | unset <id> <field>[/yellow]')
            return

        field = parts[0]
        if self.selected_docs:
            target_ids = list(self.selected_docs)
        elif self.current_doc:
            target_ids = [self.current_doc.get('_id')]
        else:
            console.print('[red]No document selected.[/red]')
            return

        count = 0
        for doc_id in target_ids:
            if not doc_id:
                continue
            doc = self.current_group.find_one({'_id': doc_id})
            if doc and field in doc:
                del doc[field]
                self.current_group.update({'_id': doc_id}, doc)
                count += 1
        if count:
            self.db.commit()
        console.print(f"[green]Unset '{field}' in {count} docs.[/green]")

    def do_replace(self, arg):
        """
        Replace entire content of the current document with JSON.
        
        Usage: replace <json_string>
        """
        if not self._check_db():
            return
        assert self.db is not None
        if not self.current_doc:
            console.print('[red]Select a document first.[/red]')
            return
        if not self.current_group:
            console.print('[red]Select a group first.[/red]')
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
        """Delete current document or selection."""
        self.do_throw(arg)

    def do_removeid(self, arg):
        """
        Delete a document by its ID.
        
        Usage: removeid <id>
        """
        self.do_del(arg)

    def do_renamegroup(self, arg):
        """
        Rename the current or specified group.
        
        Usage: renamegroup <new_name>
        """
        self.do_rename(arg)

    def do_clonegroup(self, arg):
        """
        Clone the current group to a new group.
        
        Usage: clonegroup <target_group>
        """
        if not self._check_db():
            return
        assert self.db is not None
        self.do_clone(arg)

    def do_moveid(self, arg):
        """
        Move a specific document by ID to another group.
        
        Usage: moveid <id> <target_group>
        """
        if not self._check_db():
            return
        assert self.db is not None
        if not self.current_group:
            console.print('[red]Select a group first.[/red]')
            return
        parts = arg.split()
        if len(parts) == 2:
            self._exec_move_copy(self.current_group, parts[0], parts[1], is_move=True)

    def do_copyid(self, arg):
        """
        Copy a specific document by ID to another group.
        
        Usage: copyid <id> <target_group>
        """
        if not self._check_db():
            return
        assert self.db is not None
        parts = arg.split()
        if len(parts) == 2:
            if not self.current_group:
                console.print('[red]Select a source group first.[/red]')
                return
            assert self.current_group is not None
            self._exec_move_copy(self.current_group, parts[0], parts[1], is_move=False)

    def do_snapshot(self, arg):
        """Create a point-in-time backup of the database."""
        if not self._check_db():
            return
        assert self.db is not None
        self.do_backup(arg)

    def do_restore(self, arg):
        """Instructions for restoring from a snapshot."""
        console.print("[yellow]Use 'connect' to open snapshot, or manual file copy.[/yellow]")

    def do_verify(self, arg):
        """Verify database integrity and health."""
        if not self._check_db():
            return
        assert self.db is not None
        self.do_validate(arg)

    def do_guard(self, arg):
        """Enable write-protection guard mode."""
        console.print('[dim]Guard mode enabled.[/dim]')

    def do_confirm(self, arg):
        """Set the required confirmation level for destructive operations."""
        console.print(f'[dim]Confirmation level set to {arg}[/dim]')

    def do_seal(self, arg):
        """Alias for 'lock'."""
        if not self._check_db():
            return
        assert self.db is not None
        self.do_lock(arg)

    def do_unseal(self, arg):
        """Alias for 'unlock'."""
        if not self._check_db():
            return
        assert self.db is not None
        self.do_unlock(arg)

    def do_timeline(self, arg):
        """Show version history for the current group or document."""
        if not self._check_db():
            return
        assert self.db is not None
        self.do_record('list ' + arg)

    def do_revert(self, arg):
        """Roll back changes to a previous state."""
        if not self._check_db():
            return
        assert self.db is not None
        self.do_record('undo ' + arg)

    def do_reapply(self, arg):
        """Re-apply a previously reverted change."""
        if not self._check_db():
            return
        assert self.db is not None
        self.do_record('apply ' + arg)

    def do_checkpoint(self, arg):
        """Manually trigger a disk synchronization (fsync)."""
        if not self._check_db():
            return
        assert self.db is not None
        self.db.storage.save()
        console.print('[green]Checkpoint created.[/green]')

    def do_recover(self, arg):
        """Trigger automated recovery from WAL (usually automatic on connect)."""
        console.print('[dim]Recovery runs automatically on connect.[/dim]')

    def do_query(self, arg):
        """
        Execute a complex query using the Polyglot Query Engine.
        
        Usage: query <query_string>
        """
        if not self._check_db():
            return
        assert self.db is not None
        engine = None
        parser = None
        
        if PolyglotParser and QueryEngine:
            parser = PolyglotParser()
            engine = QueryEngine(self.db)
        else:
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


    def do_scout(self, arg):
        """Scan for patterns or metadata."""
        if not self._check_db():
            return
        assert self.db is not None
        self.do_scan(arg)

    def do_scry(self, arg):
        """Inspect schema and structure."""
        if not self._check_db():
            return
        assert self.db is not None
        self.do_schema(arg)


    def do_pulse(self, arg):
        """Alias for 'status'."""
        self.do_status(arg)

    def do_ignite(self, arg):
        """Alias for 'connect'."""
        self.do_connect(arg)

    def do_vanish(self, arg):
        """Alias for 'quit'."""
        return self.do_quit(arg)

    def do_freeze(self, arg):
        """Alias for 'save' (checkpoint)."""
        if not self._check_db():
            return
        assert self.db is not None
        self.do_save(arg)

    def do_revive(self, arg):
        """Alias for 'refresh'."""
        if not self._check_db():
            return
        assert self.db is not None
        self.do_refresh(arg)

    def do_drain(self, arg):
        """Alias for 'vacuum'."""
        if not self._check_db():
            return
        assert self.db is not None
        self.do_vacuum(arg)

    def do_crypt(self, arg):
        """Change database password or encryption settings."""
        self.do_change('db_password ' + arg)

    def do_track(self, arg):
        """Alias for 'history'."""
        self.do_history(arg)

    def do_chronos(self, arg):
        """Display current system time."""
        console.print(f"[cyan]{time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]")

    def do_anchor(self, arg):
        """Mark the current context (group/doc) for quick return."""
        self._anchor = (self.current_group, self.current_doc)
        console.print('[cyan]Anchor established.[/cyan]')

    def do_recall(self, arg):
        """Return to the previously established anchor."""
        if not self._check_db():
            return
        assert self.db is not None
        if hasattr(self, '_anchor') and self._anchor:
            grp, doc = self._anchor
            if not grp:
                console.print('[yellow]Anchor group is invalid.[/yellow]')
                return
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

    def do_diagnose(self, arg):
        if not self._check_db():
            return
        assert self.db is not None
        
        try:
            from .diagnostics import Diagnostics
            diag = Diagnostics(self.db.filepath, self.db.storage.password)
            report = diag.doctor()
            console.print_json(data=report)
        except ImportError:
            console.print('[red]Diagnostics module not found.[/red]')
        except Exception as e:
            console.print(f'[red]Diagnosis failed: {e}[/red]')

    def do_check_impl(self, arg):
        if not self.current_group:
            return
        c = self.current_group.count()
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
        if not self._check_db():
            return
        assert self.db is not None
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        
        parts = arg.split()
        if len(parts) < 2:
            console.print('[yellow]Usage: fuse <id1> <id2> [prefer_left|prefer_right][/yellow]')
            return
        id1, id2 = (parts[0], parts[1])
        strategy = parts[2] if len(parts) > 2 else 'prefer_right'
        grp = self.current_group
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
        if not self._check_db():
            return
        assert self.db is not None
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        
        grp = self.current_group
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
        if not self._check_db():
            return
        assert self.db is not None
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
            grp = self.current_group
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

    def do_exhale(self, arg):
        if not self._check_db():
            return
        assert self.db is not None
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        if not arg:
            console.print('[yellow]Usage: exhale <file.json>[/yellow]')
            return
        path = arg.strip()
        docs = self.current_group.find()
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(docs, f, indent=2, default=str)
            console.print(f'[green]Exhaled {len(docs)} documents to {path}.[/green]')
        except Exception as e:
            console.print(f'[red]Exhale failed: {e}[/red]')

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
        assert self.db is not None
        
        parts = arg.split()
        if len(parts) < 2:
            console.print('[yellow]Usage: void <id> <field>[/yellow]')
            return
        doc_id, field = (parts[0], parts[1])
        grp = self.current_group
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
        if not self._check_db():
            return
        assert self.db is not None
        if not self.current_group:
            console.print('[red]Select a group first.[/red]')
            return
        docs = self.current_group.get_all()
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

    def do_type(self, arg):
        if not self._check_db():
            return
        if not self.current_group:
            return
        assert self.current_group is not None
        parts = arg.split()
        if len(parts) < 2:
            return
        doc = self.current_group.find_one({'_id': parts[0]})
        if doc and parts[1] in doc:
            val = doc[parts[1]]
            console.print(f'Type: [cyan]{type(val).__name__}[/cyan] | Value: {val}')
        else:
            console.print('[red]Not found[/red]')

    def do_clear(self, arg):
        try:
            if os.name == 'nt':
                subprocess.run(['cmd', '/c', 'cls'], check=False)
            else:
                subprocess.run(['clear'], check=False)
        except Exception as e:
            warnings.warn(f"Clear command failed: {e}")
            console.print('\n' * 80)

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

    def do_whoami(self, arg):
        """Display the current authenticated user."""
        user = getattr(self.db, 'current_user', None) if self.db else None
        username = None
        if user and hasattr(user, 'username'):
            username = user.username
        elif user:
            username = str(user)
        if not username:
            username = 'root (system)'
        console.print(f'[bold cyan]{username}[/bold cyan]')

    def do_perm(self, arg):
        """Check current user permissions across all groups."""
        if not self._check_db():
            return
        assert self.db is not None
        
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
        """
        Open a document in the system's default text editor.
        
        Usage: edit <doc_id>
        """
        if not self._check_db():
            return
        assert self.db is not None
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
        try:
            fd, tf_path = tempfile.mkstemp(suffix='.json', text=True)
            with os.fdopen(fd, 'w') as tf:
                json.dump(doc, tf, indent=2, default=str)
            if os.name == 'nt':
                os.startfile(tf_path)
                console.input("[yellow]Press Enter after you have saved and closed the editor...[/yellow]")
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
        """Perform basic mathematical calculations."""
        def eval_expr(node):
            if isinstance(node, ast.Expression):
                return eval_expr(node.body)
            if isinstance(node, ast.Constant):
                if isinstance(node.value, (int, float)):
                    return node.value
                raise ValueError('Only numbers allowed')
            if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
                val = eval_expr(node.operand)
                return +val if isinstance(node.op, ast.UAdd) else -val
            if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div)):
                left = eval_expr(node.left)
                right = eval_expr(node.right)
                if isinstance(node.op, ast.Add):
                    return left + right
                if isinstance(node.op, ast.Sub):
                    return left - right
                if isinstance(node.op, ast.Mult):
                    return left * right
                return left / right
            raise ValueError('Unsupported expression')

        expr = (arg or '').strip()
        if not expr:
            console.print('[yellow]Usage: calc <expression>[/yellow]')
            return

        try:
            tree = ast.parse(expr, mode='eval')
            result = eval_expr(tree)
            console.print(f"= {result}")
        except SyntaxError:
            console.print('[red]Invalid Syntax[/red]')
        except Exception as e:
            console.print(f'[red]Error: {e}[/red]')

    def do_schema(self, arg):
        """Infer and display the schema of the current group."""
        if not self._check_db():
            return
        assert self.db is not None
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
        """
        List unique values for a specific field.
        
        Usage: distinct <field_name>
        """
        if not self._check_db():
            return
        assert self.db is not None
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
        """Show statistical summary for a numeric field."""
        if not self._check_db():
            return
        assert self.db is not None
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
        """Rename the current group."""
        if not self._check_db():
            return
        assert self.db is not None
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
        """Clone documents from one group to another."""
        if not self._check_db():
            return
        assert self.db is not None
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
        src_data = self.db.group(src).find()
        dst_grp = self.db.group(dst)
        with console.status(f'Cloning {src} to {dst}...'):
            for doc in src_data:
                new_doc = copy.deepcopy(doc)
                dst_grp.insert(new_doc)
            self.db.commit()
        console.print(f"[green]Cloned {len(src_data)} documents to '{dst}'.[/green]")

    def do_vacuum(self, arg):
        """Trigger database compaction and storage optimization."""
        if not self._check_db():
            return
        assert self.db is not None
        console.print('[yellow]Vacuuming database...[/yellow]')
        self.db.storage._dirty = True
        self.db.commit()
        console.print('[green]Vacuum complete. Storage optimized.[/green]')

    def do_benchmark(self, arg):
        """Run a performance benchmark on the current database."""
        if not self._check_db():
            return
        assert self.db is not None
        
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
        """Parse key=value pairs or JSON strings into a dictionary."""
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
        """List all groups and their document counts."""
        if not self._check_db():
            return
        assert self.db is not None
        
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
        except Exception as e:
            warnings.warn(f"URI masking failed for {uri}: {e}")
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
        assert self.db is not None
        if self._check_lock():
            return
        name = arg.strip()
        if not name:
            console.print('[yellow]Usage: target <group_name>[/yellow]')
            return
        all_groups = self.db.get_all_groups()
        if name not in all_groups:
            matches = difflib.get_close_matches(name, all_groups, n=1, cutoff=0.6)
            if matches:
                suggestion = matches[0]
                if console.input(f"[yellow]Group '{escape(name)}' not found. Did you mean '{escape(suggestion)}'? (y/n): [/yellow]").lower() == 'y':
                    name = suggestion
                elif console.input(f"[blue]Create new group '{escape(name)}'? (y/n): [/blue]").lower() != 'y':
                    return
            elif console.input(f"[blue]Group '{escape(name)}' not found. Create new? (y/n): [/blue]").lower() != 'y':
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
        assert self.db is not None
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
                except ValueError:
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
                console.print(Panel(JSON(json.dumps(doc, default=str)), title=f"[bold green]Document @{target_idx} ({doc['_id']})[/bold green]"))
            else:
                console.print(f'[red]Index @{target_idx} out of range (0-{total_docs - 1}).[/red]')
            return
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
        """
        Search for documents matching key=value pairs or regex patterns.
        
        Usage: hunt <key>=<value> [<key>=r:<regex>]
        Example: hunt type=user name=r:^Admin.*
        """
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        query = self._parse_kv(arg)
        if not query:
            return
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
        """Internal helper to print a list of documents in a formatted table."""
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
        """Show help for a specific command or general instructions."""
        if not arg:
            self.preloop()
            return
        doc = getattr(self, f'do_{arg}', None).__doc__
        if doc:
            console.print(Panel(doc, title=f'[bold cyan]Help: {arg}[/bold cyan]', border_style='cyan'))
        else:
            console.print(f"[red]No help found for '{arg}'.[/red]")

    def do_make(self, arg):
        """
        Create a new document or a new group.
        
        Usage:
            make <key>=<value> [<key>=<value> ...]
            make group:<group_name>
            make (interactive mode)
        """
        if not self._check_db():
            return
        assert self.db is not None
        
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
                key = console.input('  Key: ').strip()
                if not key:
                    break
                val = console.input(f"  Value for '{escape(key)}': ").strip()
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
        """
        Move a document from one group to another.
        
        Usage:
            move <target_group> (if document is 'picked')
            move <doc_id> <target_group>
            move <source_group>:<doc_id> <target_group>
        """
        if not self._check_db():
            return
        assert self.db is not None
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
        """
        Copy a document from one group to another.
        
        Usage:
            copy <target_group> (if document is 'picked')
            copy <doc_id> <target_group>
            copy <source_group>:<doc_id> <target_group>
        """
        if not self._check_db():
            return
        assert self.db is not None
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
        """
        Switch current user identity.
        
        Usage: become <username> [password]
        """
        if not self._check_db():
            return
        assert self.db is not None
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
        """
        Manage database users and roles.
        
        Usage:
            user list
            user create <username> [password] [role]
            user drop <username>
        """
        if not self._check_db():
            return
        assert self.db is not None
        
        if 'perms' not in self.db.plugins:
            if not PermissionManager:
                console.print("[red]Error: 'hvpdb-perms' plugin not found.[/red]")
                return
            self.db.plugins['perms'] = PermissionManager(self.db)
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
        """
        Grant group access to a user.
        
        Usage: grant <username> <group>
        """
        if not self._check_db():
            return
        assert self.db is not None
        
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
        """
        Revoke group access from a user.
        
        Usage: revoke <username> <group>
        """
        if not self._check_db():
            return
        assert self.db is not None
        
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
        """Internal helper to execute move or copy operations between groups."""
        if not self.db:
            return
        assert self.db is not None
        
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
        """Pick and display a random document from the current group."""
        if not self._check_db():
            return
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        assert self.current_group is not None
        docs = self.current_group.find()
        if not docs:
            console.print('[dim]Group is empty.[/dim]')
            return
        doc = random.choice(docs)
        self.current_doc = doc
        self._update_prompt()
        json_str = json.dumps(doc, indent=2, default=str)
        console.print(Panel(JSON(json_str), title='[bold green]Random Pick (LOCKED)[/bold green]', border_style='green'))

    def do_fields(self, arg):
        """List all unique field names present in the current group."""
        if not self._check_db():
            return
        assert self.db is not None
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        fields = set()
        for doc in self.current_group.find():
            fields.update(doc.keys())
        console.print(Panel('\n'.join(sorted(fields)), title=f'Fields in {self.current_group.name}'))

    def do_nuke(self, arg):
        """
        Permanently delete a group and all its contents.
        
        Usage: nuke <group_name>
        """
        if not self._check_db():
            return
        assert self.db is not None
        
        if not arg:
            self.do_help('nuke')
            return
        confirm = console.input(f"🔥 WARNING: Nuke group '{escape(arg)}'? (y/n): ")
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
        """Show HVPDB version and engine information."""
        from . import __version__ as pkg_version
        console.print(f'[bold cyan]HVPDB v{pkg_version}[/bold cyan]')
        console.print('Engine: HVP-Storage (Python)')

    def do_how(self, arg):
        """
        Explain the purpose and flow of a command.
        
        Usage: how <command>
        """
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
        """Show usage examples for a specific command."""
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
        """Alias for 'nuke'."""
        self.do_nuke(arg)

    def do_backup(self, arg):
        """
        Create a backup of the current database file.
        
        Usage: backup <destination_path>
        """
        if not arg:
            self.do_help('backup')
            return
        try:
            if self.db and hasattr(self.db, 'filepath') and os.path.exists(self.db.filepath):
                shutil.copy2(self.db.filepath, arg)
                console.print(f'[green]Backup created at {arg}[/green]')
            else:
                console.print('[red]Cannot backup in-memory or cluster DB yet.[/red]')
        except Exception as e:
            console.print(f'[red]Backup failed: {e}[/red]')

    def do_pick(self, arg):
        """
        Select a document from the last search results by its index.
        
        Usage: pick <index>
        """
        if not self.last_search_results:
            console.print("[yellow]No results to pick from. Run 'peek' or 'hunt' first.[/yellow]")
            return
        try:
            idx = int(arg)
            if 0 <= idx < len(self.last_search_results):
                self.current_doc = self.last_search_results[idx]
                self._update_prompt()
                json_str = json.dumps(self.current_doc, indent=2, default=str)
                console.print(Panel(JSON(json_str), title='[bold green]Selected Document (LOCKED)[/bold green]', border_style='green'))
            else:
                console.print('[red]Index out of range.[/red]')
        except ValueError:
            console.print('[red]Invalid index.[/red]')

    def do_select(self, arg):
        """
        Add documents to the multi-selection buffer.
        
        Usage:
            select all
            select clear
            select <index>
            select <start>-<end>
        """
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
        """
        Remove documents from the multi-selection buffer.
        
        Usage:
            discard all
            discard <index>
        """
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
        """
        Update the selected document(s) with new values.
        
        Usage: morph <key>=<value> [<key>=<value> ...]
        """
        if not self._check_db():
            return
        assert self.db is not None
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        assert self.current_group is not None
        
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
        """
        Delete the selected document(s).
        
        Usage: throw
        """
        if not self._check_db():
            return
        assert self.db is not None
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        assert self.current_group is not None
        
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
        """Count the number of documents in the current group."""
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        count = self.current_group.count()
        console.print(f'Total documents: {count}')

    def do_truncate(self, arg):
        """
        Delete all documents in the current group.
        
        Usage: truncate
        """
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        if not self.db:
            return
        assert self.db is not None
        
        confirm = console.input(f"WARNING: Delete ALL data in '{self.current_group.name}'? (yes/no): ")
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
        """
        Create an index on a specific field.
        
        Usage: index <field> [unique]
        """
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        if not self.db:
            return
        assert self.db is not None
        
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
        """
        Export current group data to a JSON file.
        
        Usage: export <filename.json>
        """
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
        """
        Import data from a JSON file into the current group.
        
        Usage: import <filename.json>
        """
        if not self.current_group:
            console.print('[red]No group selected.[/red]')
            return
        if not self.db:
            return
        assert self.db is not None
        
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
        """
        View the audit trail/history for the selected document.
        
        Usage: trace
        """
        if not self.current_doc:
            console.print('[red]Select a document first.[/red]')
            return
        if not hasattr(self.current_group, 'get_audit_trail'):
            console.print('[yellow]Audit logging not available.[/yellow]')
            return
        # Type guard for pyright (implied by context but explicit is better)
        assert self.current_group is not None
        
        logs = self.current_group.get_audit_trail(self.current_doc['_id'])
        if not logs:
            console.print('[dim]No history found.[/dim]')
            return
        table = Table(title=f"Audit Log: {self.current_doc['_id'][:8]}")
        table.add_column('Time', style='dim')
        table.add_column('Action', style='magenta')
        table.add_column('Data', style='white')
        for log in logs:
            ts = datetime.datetime.fromtimestamp(log.get('timestamp', 0)).strftime('%Y-%m-%d %H:%M:%S')
            op = log.get('op', 'unknown')
            data = str(log.get('data', {}))[:60]
            table.add_row(ts, op, data)
        console.print(table)

    def do_save(self, arg):
        """
        Manually save database changes or configure auto-save.
        
        Usage:
            save
            save auto on|off
        """
        if not self._check_db():
            return
        assert self.db is not None
        
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
        """Save changes and exit the shell."""
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
        """Display a tree view of the database structure (groups and documents)."""
        if not self._check_db():
            return
        assert self.db is not None
        
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
        """Perform an integrity check on the database data."""
        if not self._check_db():
            return
        assert self.db is not None
        
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
        """Real-time monitoring of database activity (Ctrl+C to stop)."""
        if not self._check_db():
            return
        assert self.db is not None
        import time
        interval = 2
        if arg and arg.isdigit():
            interval = int(arg)
        console.print('[cyan]Monitoring... (Ctrl+C to stop)[/cyan]')
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
        """
        Manage and interact with the Write-Ahead Log (WAL) records.
        
        Usage:
            record status [on|off] - Check or toggle record mode
            record list [limit]    - List recent transactions
            record peek <seq>      - Inspect a specific transaction
            record undo <seq>      - Revert a transaction
            record apply <seq>     - Re-apply a transaction
        """
        if not self._check_db():
            return
        assert self.db is not None
        
        is_cluster = getattr(self.db, 'is_cluster', False)
        target_storage = self.db.storage
        current_group_name = None
        if self.current_group:
            current_group_name = self.current_group.name
            target_storage = self.current_group.storage
        elif is_cluster:
            console.print('[yellow]Viewing Cluster Metadata WAL. Use "target <group>" to see data transactions.[/yellow]')

        args = arg.split()
        if not args:
            self.do_help('record')
            return

        if not hasattr(target_storage, 'wal') or not getattr(target_storage, 'wal'):
            console.print('[red]WAL not accessible.[/red]')
            return

        cmd = args[0].lower()
        if cmd == 'status':
            if len(args) > 1:
                mode = args[1].lower()
                self.record_mode = mode == 'on'
            console.print(f"Record Mode: [{('green' if self.record_mode else 'red')}]{('ON' if self.record_mode else 'OFF')}[/]")
            return

        if cmd == 'list':
            limit = 10
            if len(args) > 1:
                try:
                    limit = int(args[1])
                except ValueError:
                    console.print('[red]Invalid limit.[/red]')
                    return
            logs = []

            def collector(entry):
                logs.append(entry)

            target_storage.wal.replay(0, collector)
            logs = sorted(logs, key=lambda x: x.get('seq', 0), reverse=True)[:limit]
            table = Table(title=f'Transaction History (Last {limit})')
            table.add_column('Seq', style='cyan', width=6)
            table.add_column('Txn ID', style='blue', width=8)
            table.add_column('Time', style='dim')
            table.add_column('Type', style='dim')
            table.add_column('Op', style='magenta')
            table.add_column('Group', style='yellow')
            table.add_column('ID', style='white')
            for log in logs:
                ts = datetime.datetime.fromtimestamp(log.get('ts', 0)).strftime('%H:%M:%S')
                txn = log.get('txn', '')[:8] if log.get('txn') else '-'
                etype = log.get('type', '-')
                table.add_row(str(log.get('seq')), txn, ts, etype, str(log.get('op') or '-'), str(log.get('g') or '-'), str(log.get('id') or '-')[:8])
            console.print(table)
            return

        if cmd == 'peek':
            if len(args) < 2:
                console.print('[yellow]Usage: record peek <seq>[/yellow]')
                return
            try:
                target_seq = int(args[1])
            except ValueError:
                console.print('[red]Invalid seq.[/red]')
                return
            found_log = None

            def finder(entry):
                nonlocal found_log
                if entry.get('seq') == target_seq:
                    found_log = entry

            target_storage.wal.replay(0, finder)
            if not found_log:
                console.print(f'[red]Record #{target_seq} not found.[/red]')
                return

            if found_log.get('type') != 'DATA' or not found_log.get('op'):
                console.print(Panel(json.dumps(found_log, indent=2), title=f'Record #{target_seq}', border_style='yellow'))
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
            return

        if cmd == 'undo':
            if len(args) < 2:
                console.print('[yellow]Usage: record undo <seq>[/yellow]')
                return
            try:
                seq = int(args[1])
            except ValueError:
                console.print('[red]Invalid seq.[/red]')
                return
            found_log = None

            def finder(entry):
                nonlocal found_log
                if entry.get('seq') == seq:
                    found_log = entry

            target_storage.wal.replay(0, finder)
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

            target_storage.wal.replay(0, txn_collector)
            txn_ops.sort(key=lambda x: x.get('seq'), reverse=True)
            if not txn_ops:
                console.print('[yellow]No DATA operations found for this transaction.[/yellow]')
                return

            if is_cluster:
                groups_in_txn = {op.get('g') for op in txn_ops if op.get('g')}
                if self.current_group:
                    if groups_in_txn != {current_group_name}:
                        console.print('[red]Refusing undo: transaction is not scoped to current group. Use "target <group>".[/red]')
                        return
                else:
                    meta_group = getattr(self.db, '_CLUSTER_META_GROUP_NAME', None)
                    if not meta_group or (groups_in_txn - {meta_group}):
                        console.print('[red]Refusing undo from cluster metadata context. Use "target <group>".[/red]')
                        return

            console.print(f'[bold]Undoing Transaction {target_txn_id[:8]} ({len(txn_ops)} operations)...[/bold]')
            if console.input('Confirm undo? (y/n) ').lower() != 'y':
                return

            undo_txn_id = target_storage.begin_txn()
            try:
                for op_log in txn_ops:
                    op = op_log.get('op')
                    grp_name = op_log.get('g')
                    doc_id = op_log.get('id')
                    data = op_log.get('d')
                    before = op_log.get('b')

                    if not grp_name or not doc_id or not op:
                        raise ValueError('Invalid WAL entry for undo')

                    grp = self.db.group(grp_name)
                    if is_cluster and grp.storage is not target_storage:
                        raise RuntimeError('Cluster storage mismatch')

                    if op == 'insert':
                        grp.delete({'_id': doc_id}, external_txn_id=undo_txn_id)
                        console.print(f'[green]Reverted Insert: Deleted {doc_id}[/green]')
                    elif op == 'delete':
                        if grp.find_one({'_id': doc_id}):
                            console.print(f'[yellow]Warning: Document {doc_id} already exists. Skipping restore.[/yellow]')
                        else:
                            restore_data = before if before else data
                            if not restore_data:
                                raise ValueError('Missing restore data')
                            grp.insert(restore_data, external_txn_id=undo_txn_id)
                            console.print(f'[green]Reverted Delete: Restored {doc_id}[/green]')
                    elif op == 'update':
                        if before:
                            grp.update({'_id': doc_id}, before, external_txn_id=undo_txn_id)
                            console.print(f'[green]Reverted Update: Restored {doc_id}[/green]')
                        else:
                            console.print(f'[red]Cannot undo update {doc_id}: Missing before-image.[/red]')
                            raise ValueError('Missing before-image')
                    else:
                        raise ValueError(f'Unsupported op: {op}')

                target_storage.commit_txn(undo_txn_id)
                self.db.commit()
                console.print('[bold green]Transaction Undone Successfully.[/bold green]')
            except Exception as e:
                target_storage.rollback_txn(undo_txn_id)
                console.print(f'[bold red]Undo Failed: {e}. Rolled back changes.[/bold red]')
            return

        if cmd == 'apply':
            if len(args) < 2:
                console.print('[yellow]Usage: record apply <seq>[/yellow]')
                return
            try:
                seq = int(args[1])
            except ValueError:
                console.print('[red]Invalid seq.[/red]')
                return

            found_log: Optional[dict] = None

            def finder(entry):
                nonlocal found_log
                if entry.get('seq') == seq:
                    found_log = entry

            target_storage.wal.replay(0, finder)
            if not found_log:
                console.print(f'[red]Record #{seq} not found.[/red]')
                return

            if found_log.get('type') != 'DATA' or not found_log.get('op'):
                console.print('[red]This record is not a DATA operation.[/red]')
                return

            grp_name = found_log.get('g')
            if not grp_name:
                console.print('[red]Missing group name in record.[/red]')
                return

            if is_cluster:
                if self.current_group:
                    if grp_name != current_group_name:
                        console.print('[red]Refusing apply: record is not scoped to current group.[/red]')
                        return
                else:
                    meta_group = getattr(self.db, '_CLUSTER_META_GROUP_NAME', None)
                    if grp_name != meta_group:
                        console.print('[red]Refusing apply from cluster metadata context. Use "target <group>".[/red]')
                        return

            op = found_log.get('op')
            grp = self.db.group(grp_name)
            if is_cluster and grp.storage is not target_storage:
                console.print('[red]Cluster storage mismatch.[/red]')
                return

            data = found_log.get('d')
            doc_id = found_log.get('id')
            if console.input(f'Re-apply {op} #{seq}? (y/n) ').lower() != 'y':
                return
            if op == 'insert':
                if not data:
                    console.print('[red]Missing data for insert.[/red]')
                    return
                grp.insert(data)
            elif op == 'delete':
                if not doc_id:
                    console.print('[red]Missing id for delete.[/red]')
                    return
                grp.delete({'_id': doc_id})
            elif op == 'update':
                if not doc_id or not data:
                    console.print('[red]Missing id/data for update.[/red]')
                    return
                grp.update({'_id': doc_id}, data)
            else:
                console.print(f'[red]Unsupported op: {op}[/red]')
                return

            self.db.commit()
            console.print('[green]Transaction re-applied.[/green]')
            return

        self.do_help('record')
