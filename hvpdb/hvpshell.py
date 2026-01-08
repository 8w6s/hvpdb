
import cmd
import shlex
import json
import os
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.tree import Tree
from .core import HVPDB

console = Console()

class HVPShell(cmd.Cmd):
    intro = None # Disable default intro print to use Rich Panel in preloop
    prompt = "hvpdb > "
    
    def __init__(self, db: HVPDB = None):
        super().__init__()
        self.db = db
        self.current_group = None
        self.prev_group = None # History for 'jump'
        self.current_doc = None
        self.is_locked = False # Safety Lock State
        self.last_search_results = []
        self._cmd_history = []
        self.record_mode = True # Default to recording enabled (visualization only)
        self.auto_save = False # Auto-save on exit (Default: Off -> Ask)

    def preloop(self):
        """Print the welcome banner using Rich."""
        banner_text = """
    [bold]Connection:[/bold]
    - [green]connect[/green] <path>    : Connect to database
    - [green]disconnect[/green]        : Disconnect current DB

    [bold]Navigation:[/bold]
    - [green]scan[/green]           : List all groups
    - [green]target[/green] <group> : Select a group context
    
    [bold]Data Operations:[/bold]
    - [green]peek[/green]           : View all documents
    - [green]hunt[/green] k=v       : Search documents
    - [green]make[/green] k=v       : Create new document
    - [green]check[/green]          : Count documents
    - [green]truncate[/green]       : Delete all documents in group
    - [green]import/export[/green]  : Data migration
    - [green]distinct[/green] <f>   : List unique values
    - [green]stats[/green] <f>      : Calculate statistics
    
    [bold]Group Operations:[/bold]
    - [green]rename[/green] <name>  : Rename current group
    - [green]clone[/green] <src> <dst>: Clone a group
    
    [bold]Item Operations (After 'pick'):[/bold]
    - [green]pick[/green] <index>   : Select document from list
    - [green]morph[/green] k=v      : Update selected document
    - [green]throw[/green]          : Delete selected document
    
    [bold]Audit & Version Control:[/bold]
    - [green]record[/green]           : Data versioning (undo/redo)
    - [green]trace[/green]            : View audit log
    
    [bold]System & Maintenance:[/bold]
    - [green]save[/green]             : Save to disk
    - [green]refresh[/green]          : Reload from disk
    - [green]perm[/green]             : Check permissions
    - [green]index[/green] <field>  : Create index
    - [green]schema[/green]         : Infer schema
    - [green]vacuum[/green]         : Compact storage
    - [green]validate[/green]       : Check DB integrity
    - [green]benchmark[/green]      : Run performance test
    - [green]monitor[/green]        : Realtime dashboard
    - [green]status[/green]         : Database info
    - [green]history[/green]        : Show command history
    - [green]clear[/green]          : Clear screen
    - [green]quit[/green]           : Exit

    [dim]Tip: Type 'help <command>' for detailed usage.[/dim]
        """
        console.print(Panel(
            banner_text.strip(), 
            title="[bold cyan]HVPDB Ops Shell (HVPShell)[/bold cyan]",
            subtitle="[dim]Modern Command-Line Interface[/dim]",
            border_style="cyan"
        ))
        self._update_prompt()

    def do_connect(self, arg):
        """
        Connect to a database.
        
        Usage: connect <path_or_uri> [password]
        Example: connect my_db.hvp secret123
        """
        if self.db:
            console.print("[yellow]Already connected. Use 'disconnect' first.[/yellow]")
            return
            
        args = arg.split()
        if not args:
            console.print("[red]Usage: connect <path> [password][/red]")
            return
            
        path = args[0]
        password = args[1] if len(args) > 1 else None
        
        # Auto-append extension
        if not path.startswith("hvp://") and not path.endswith(".hvp") and not path.endswith(".hvdb"):
             path += ".hvp"
             
        try:
            self.db = HVPDB(path, password)
            console.print(f"[green]Connected to {path}[/green]")
            self._update_prompt()
        except Exception as e:
            console.print(f"[red]Connection failed: {e}[/red]")

    def do_disconnect(self, arg):
        """
        Disconnect from current database (Hard Reset).
        Clears all context, locks, and history references.
        """
        if not self.db:
            console.print("[yellow]Not connected.[/yellow]")
            return
            
        try:
            self.db.close()
        except:
            pass
            
        # HARD RESET STATE
        self.db = None
        self.current_group = None
        self.current_doc = None
        self.is_locked = False
        self.last_search_results = [] # Clear search cache to prevent access to old objects
        
        console.print("[green]Disconnected. Context cleared.[/green]")
        self._update_prompt()

    def do_refresh(self, arg):
        """
        Refresh data from disk (Reload).
        Useful if the database file was modified by another process.
        """
        if not self.db:
             console.print("[yellow]Not connected.[/yellow]")
             return
        
        try:
            self.db.refresh()
            console.print("[green]Database refreshed successfully.[/green]")
            # Re-select group to ensure stale references are updated
            if self.current_group:
                self.current_group = self.db.group(self.current_group.name)
        except Exception as e:
            console.print(f"[red]Refresh failed: {e}[/red]")

    def _check_db(self):
        """Helper to check if DB is connected."""
        if not self.db:
            console.print("[red]Not connected to any database. Use 'connect <path>' first.[/red]")
            return False
        return True

    def cmdloop(self, intro=None):
        """Override cmdloop to use Rich input for colored prompt."""
        self.preloop()
        
        # Setup completion if available
        if self.use_rawinput and self.completekey:
            try:
                import readline
                self.old_completer = readline.get_completer()
                readline.set_completer(self.complete)
                readline.parse_and_bind(self.completekey+": complete")
            except ImportError:
                pass

        stop = None
        while not stop:
            try:
                if self.use_rawinput:
                    # Use rich console.input to render colored prompt
                    # Note: console.input prints prompt then calls input()
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
                console.print("^C")
            except EOFError:
                console.print("^D")
                break
            except Exception as e:
                console.print(f"[red]Error: {e}[/red]")

        self.postloop()

    def precmd(self, line):
        """Hook before command execution to log history."""
        if line and line != 'history':
            self._cmd_history.append(line)
        return line

    def do_history(self, arg):
        """Show command history."""
        if not self._cmd_history:
            console.print("[dim]No history yet.[/dim]")
            return
        for i, cmd in enumerate(self._cmd_history[-20:]): # Show last 20
            console.print(f"{i+1}. {cmd}")

    def do_clear(self, arg):
        """Clear the screen."""
        os.system('cls' if os.name == 'nt' else 'clear')

    def do_cls(self, arg):
        """Alias for clear."""
        self.do_clear(arg)

    # --- Auto Completion Logic (Optimized) ---
    def _complete_groups(self, text, line, begidx, endidx):
        """Auto-complete group names."""
        if not self.db: return []
        # TODO: Add caching here if get_all_groups becomes slow
        groups = self.db.get_all_groups()
        if not text:
            return groups
        return [g for g in groups if g.startswith(text)]

    def _complete_fields(self, text, line, begidx, endidx):
        """Auto-complete field names based on schema inference."""
        if not self.current_group:
            return []
            
        # Optimization: Only infer if we have text or minimal sampling
        # Ideally, we should cache schema per group session
        fields = set()
        # Limit sampling to 5 docs for speed during completion
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
        """
        Switch current user context (su).
        Usage: become <username>
        """
        if not self._check_db(): return
        
        username = arg.strip()
        if not username:
            console.print("[red]Usage: become <username>[/red]")
            return
            
        # Verify user exists first (to avoid prompt if not needed, though security wise timing attack etc... 
        # but this is embedded DB shell)
        if username not in self.db.storage.data.get("users", {}):
             console.print(f"[red]User '{username}' does not exist.[/red]")
             return
             
        import getpass
        password = console.input(f"Password for [cyan]{username}[/cyan]: ", password=True)
        
        if self.db.authenticate(username, password):
            console.print(f"[green]Authenticated as {username}[/green]")
            self._update_prompt()
        else:
            console.print("[red]Authentication failed: Invalid password.[/red]")

    def do_whoami(self, arg):
        """Show current user."""
        # Fix: Access core property correctly
        user = getattr(self.db, 'current_user', None)
        
        # In case current_user is just a string in some versions or object in others
        username = user
        if hasattr(user, 'username'):
            username = user.username
            
        if not username:
            username = "root (system)"
            
        console.print(f"[bold cyan]{username}[/bold cyan]")

    def do_perm(self, arg):
        """
        Check permissions for the current user.
        Usage: perm
        """
        if not self._check_db(): return

        username = getattr(self.db, 'current_user', None)
        if not username:
            console.print("[bold red]Current User: root (System Admin)[/bold red]")
            console.print("[dim]Root has full access to all groups.[/dim]")
            return

        # Fetch user details
        user_data = self.db.storage.data.get("users", {}).get(username)
        if not user_data:
            console.print(f"[red]Error: User record for '{username}' not found.[/red]")
            return

        role = user_data.get("role", "user")
        allowed_groups = user_data.get("groups", [])
        
        console.print(Panel(
            f"User: [bold cyan]{username}[/bold cyan]\nRole: [magenta]{role.upper()}[/magenta]",
            title="Permission Check",
            border_style="cyan"
        ))

        # Check access against all available groups
        all_groups = self.db.get_all_groups()
        if not all_groups:
            console.print("[yellow]No groups found in database.[/yellow]")
            return

        table = Table(title="Group Access Control")
        table.add_column("Group Name", style="white")
        table.add_column("Access", justify="center")
        table.add_column("Reason", style="dim")

        for grp in all_groups:
            has_access = False
            reason = "Denied"
            
            if role == "admin":
                has_access = True
                reason = "Admin Role"
            elif "*" in allowed_groups:
                has_access = True
                reason = "Wildcard (*)"
            elif grp in allowed_groups:
                has_access = True
                reason = "Explicit Grant"
            
            status = "[green]✅ ALLOWED[/green]" if has_access else "[red]❌ DENIED[/red]"
            table.add_row(grp, status, reason)
            
        console.print(table)

    def do_edit(self, arg):
        """
        Edit a document in external editor.
        
        Usage: edit <doc_id>
        """
        if not self.current_group:
            console.print("[red]No group selected.[/red]")
            return
            
        if not arg:
            console.print("[yellow]Usage: edit <doc_id>[/yellow]")
            return

        doc = self.current_group.find_one({"_id": arg})
        if not doc:
            console.print(f"[red]Document {arg} not found.[/red]")
            return

        # Create temp file
        import tempfile
        import subprocess
        
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as tf:
            json.dump(doc, tf, indent=2, default=str)
            tf_path = tf.name

        try:
            # Open editor
            if os.name == 'nt':
                os.system(f"notepad {tf_path}")
            else:
                editor = os.environ.get('EDITOR', 'vim')
                subprocess.call([editor, tf_path])
                
            # Read back
            with open(tf_path, 'r') as tf:
                new_doc = json.load(tf)
                
            # Update if changed
            if new_doc != doc:
                # Ensure ID didn't change (or handle it)
                if new_doc.get("_id") != doc["_id"]:
                     console.print("[red]Error: Cannot change _id.[/red]")
                else:
                    self.current_group.update({"_id": doc["_id"]}, new_doc)
                    self.db.commit()
                    console.print("[green]Document updated successfully via editor.[/green]")
            else:
                console.print("[dim]No changes made.[/dim]")
                
        except Exception as e:
            console.print(f"[red]Edit failed: {e}[/red]")
        finally:
            if os.path.exists(tf_path):
                os.remove(tf_path)

    def do_schema(self, arg):
        """Infer and show schema of current group."""
        if not self.current_group:
            console.print("[red]No group selected.[/red]")
            return
            
        docs = self.current_group.find()
        if not docs:
            console.print("[dim]Group is empty. Cannot infer schema.[/dim]")
            return
            
        schema = {}
        for doc in docs[:100]: # Sample first 100 docs
            for k, v in doc.items():
                t = type(v).__name__
                if k not in schema:
                    schema[k] = {t}
                else:
                    schema[k].add(t)
                    
        table = Table(title=f"Schema Inference: {self.current_group.name}")
        table.add_column("Field", style="cyan")
        table.add_column("Types", style="green")
        
        for k, types in schema.items():
            table.add_row(k, ", ".join(types))
        console.print(table)

    def do_distinct(self, arg):
        """List unique values for a field: distinct role"""
        if not self.current_group:
            console.print("[red]No group selected.[/red]")
            return
        if not arg:
            console.print("[yellow]Usage: distinct <field_name>[/yellow]")
            return
            
        docs = self.current_group.find()
        values = set()
        for doc in docs:
            if arg in doc:
                # Make unhashable types hashable (str representation)
                val = doc[arg]
                if isinstance(val, (dict, list)):
                    val = str(val)
                values.add(val)
                
        console.print(f"[bold]Unique values for '{arg}':[/bold]")
        for v in sorted(list(values), key=lambda x: str(x)):
            console.print(f"- {v}")

    def do_stats(self, arg):
        """Calculate stats for a numeric field: stats age"""
        if not self.current_group:
            console.print("[red]No group selected.[/red]")
            return
        if not arg:
            console.print("[yellow]Usage: stats <field_name>[/yellow]")
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
        console.print(Panel(f"""
        Statistics for '{arg}'
        --------------------
        Count: {len(values)}
        Min  : {min(values)}
        Max  : {max(values)}
        Sum  : {sum(values)}
        Avg  : {avg:.2f}
        """, title="Stats"))

    def do_rename(self, arg):
        """Rename current group: rename new_name"""
        if not self.current_group:
            console.print("[red]No group selected.[/red]")
            return
        if not arg:
            console.print("[yellow]Usage: rename <new_name>[/yellow]")
            return
            
        old_name = self.current_group.name
        
        # Simple rename logic for single file mode
        if hasattr(self.db, 'is_cluster') and self.db.is_cluster:
             console.print("[yellow]Rename not supported in cluster mode yet.[/yellow]")
             return
             
        if arg in self.db.storage.data["groups"]:
            console.print(f"[red]Group '{arg}' already exists.[/red]")
            return
            
        # Move data
        self.db.storage.data["groups"][arg] = self.db.storage.data["groups"].pop(old_name)
        
        # Move indexes if any
        if "_indexes" in self.db.storage.data and old_name in self.db.storage.data["_indexes"]:
            self.db.storage.data["_indexes"][arg] = self.db.storage.data["_indexes"].pop(old_name)
            
        self.db.storage._dirty = True
        self.db.commit()
        
        # Update context
        self.current_group = self.db.group(arg)
        self.prompt = f"hvpdb:{arg} > "
        console.print(f"[green]Renamed '{old_name}' to '{arg}'.[/green]")

    def do_clone(self, arg):
        """Clone group: clone source dest"""
        args = arg.split()
        if len(args) != 2:
            console.print("[yellow]Usage: clone <source_group> <dest_group>[/yellow]")
            return
            
        src, dst = args
        if src not in self.db.get_all_groups():
            console.print(f"[red]Source group '{src}' not found.[/red]")
            return
        if dst in self.db.get_all_groups():
            console.print(f"[red]Destination group '{dst}' already exists.[/red]")
            return
            
        # Deep copy data
        import copy
        src_data = self.db.group(src).find()
        dst_grp = self.db.group(dst)
        
        with console.status(f"Cloning {src} to {dst}..."):
            for doc in src_data:
                new_doc = copy.deepcopy(doc)
                # Keep ID or generate new? Clone usually keeps ID but might conflict if in same group. 
                # Since different group, keeping ID is fine.
                dst_grp.insert(new_doc)
            self.db.commit()
            
        console.print(f"[green]Cloned {len(src_data)} documents to '{dst}'.[/green]")

    def do_vacuum(self, arg):
        """Compact storage."""
        console.print("[yellow]Vacuuming database...[/yellow]")
        self.db.storage._dirty = True
        self.db.commit()
        console.print("[green]Vacuum complete. Storage optimized.[/green]")

    def do_benchmark(self, arg):
        """Run performance benchmark."""
        import time
        console.print("[bold cyan]Running Benchmark...[/bold cyan]")
        
        # Create temp group
        bench_grp = self.db.group("_benchmark_temp")
        
        # 1. Write Test
        start = time.time()
        count = 1000
        
        # Optimize: Use Transaction for bulk insert
        txn = self.db.begin()
        try:
            with txn:
                for i in range(count):
                    bench_grp.insert({"id": i, "data": "x"*100})
        except Exception as e:
            console.print(f"[red]Write failed: {e}[/red]")
            # Cleanup
            if "_benchmark_temp" in self.db._groups:
                del self.db._groups["_benchmark_temp"]
            return

        duration = time.time() - start
        w_ops = count / duration
        console.print(f"Write: {w_ops:.2f} ops/sec ({count} docs)")
        
        # 2. Read Test
        start = time.time()
        bench_grp.find()
        duration = time.time() - start
        r_ops = count / duration
        console.print(f"Read : {r_ops:.2f} ops/sec")
        
        # Cleanup
        if hasattr(self.db, 'is_cluster') and self.db.is_cluster:
             pass # Skip cleanup for safety in cluster
        else:
             # Fix: Remove from storage AND cache to prevent stale reference bug
             if "_benchmark_temp" in self.db.storage.data["groups"]:
                 del self.db.storage.data["groups"]["_benchmark_temp"]
             if "_benchmark_temp" in self.db._groups:
                 del self.db._groups["_benchmark_temp"]
             self.db.commit()
             
        console.print("[green]Benchmark finished.[/green]")

    def _parse_kv(self, args):
        """Smart Parse: Automatically detect JSON or Key=Value format."""
        args = args.strip()
        if not args:
            return {}
            
        # 1. Try JSON Parsing first if it looks like JSON
        if args.startswith("{"):
            try:
                return json.loads(args)
            except json.JSONDecodeError:
                console.print("[yellow]Invalid JSON format. Falling back to key=value parsing...[/yellow]")
        
        # 2. Fallback to Key=Value parsing
        data = {}
        try:
            parts = shlex.split(args)
            for part in parts:
                if "=" in part:
                    k, v = part.split("=", 1)
                    if v.isdigit():
                        v = int(v)
                    elif v.lower() == "true":
                        v = True
                    elif v.lower() == "false":
                        v = False
                    data[k] = v
        except Exception as e:
            console.print(f"[red]Syntax Error: {e}[/red]")
            return None
        return data

    def do_scan(self, arg):
        """List all groups."""
        if not self._check_db(): return
        if self._check_lock(): return # Prevent scanning if locked
        
        groups = self.db.get_all_groups()
        if not groups:
            console.print("[dim]No groups found.[/dim]")
            return
        
        table = Table(title="Groups")
        table.add_column("Name", style="cyan")
        table.add_column("Documents", style="green")
        
        for g in groups:
            count = self.db.group(g).count()
            table.add_row(g, str(count))
        console.print(table)

    def _mask_uri(self, uri: str) -> str:
        """
        Mask URI for security display (Rule-based).
        
        Rules:
        - Local File: Show filename only.
        - IPv4: Show first/last octet (192.***.***.50).
        - Domain: Show first/last part (db.***.com).
        - Port: Show first digit only (5***).
        - IPv6: Show first/last block (fe80::...::1).
        """
        if "://" not in uri:
            return os.path.basename(uri) 
            
        try:
            from urllib.parse import urlparse
            parsed = urlparse(uri)
            
            # Mask Host
            host = parsed.hostname
            if not host:
                masked_host = "unknown"
            else:
                # IPv4 Check
                parts = host.split('.')
                if len(parts) == 4 and all(p.isdigit() for p in parts):
                    masked_host = f"{parts[0]}.***.***.{parts[3]}"
                # Domain Check
                elif len(parts) > 2:
                    masked_host = f"{parts[0]}.***.{parts[-1]}"
                # IPv6 or Simple Host
                else:
                    masked_host = f"{host[:4]}...{host[-2:]}" if len(host) > 6 else "***"

            # Mask Port
            port = str(parsed.port) if parsed.port else ""
            if port:
                masked_port = port[0] + "*" * (len(port) - 1)
                netloc = f"{masked_host}:{masked_port}"
            else:
                netloc = masked_host
                
            return f"{parsed.scheme}://{netloc}{parsed.path}"
        except:
            return "******"

    def _update_prompt(self):
        """Update the prompt with multi-level state reflection."""
        if not self.db:
            self.prompt = "[bold red]hvpdb (disconnected)[/bold red] > "
            return

        # Level 1: Connection
        conn_info = self._mask_uri(self.db.filepath)
        prompt_parts = [f"[bold cyan]hvpdb[/bold cyan] [[dim white]{conn_info}[/dim white]]"]
        
        # Level 2: Group Context
        if self.current_group:
            prompt_parts.append(f"[[yellow]{self.current_group.name}[/yellow]]")
            
            # Level 3: Document Context
            if self.current_doc:
                doc_id = self.current_doc.get("_id", "unknown")[:6]
                prompt_parts.append(f"[[magenta]{doc_id}[/magenta]]")
        
        # Level 4: Status Flags
        if self.is_locked:
            prompt_parts.append("[bold red][LOCKED][/bold red]")
            
        self.prompt = " ".join(prompt_parts) + " > "

    def do_lock(self, arg):
        """
        Lock the current context.
        
        Prevents navigation commands (target, scan, cancel) to ensure
        operations are performed ONLY on the currently selected group/document.
        """
        if self.is_locked:
            console.print("[yellow]Already locked.[/yellow]")
            return
        
        if not self.current_group:
            console.print("[red]Cannot lock at root level. Select a group first.[/red]")
            return
            
        self.is_locked = True
        self._update_prompt()
        console.print("[bold red]🔒 Context LOCKED. Navigation disabled until 'unlock'.[/bold red]")

    def do_unlock(self, arg):
        """Unlock the current context."""
        if not self.is_locked:
            console.print("[yellow]Not locked.[/yellow]")
            return
            
        self.is_locked = False
        self._update_prompt()
        console.print("[green]🔓 Context UNLOCKED.[/green]")

    def _check_lock(self):
        """Helper to check lock state before navigation."""
        if self.is_locked:
            console.print("[bold red]⛔ Action blocked by Safety Lock. Type 'unlock' first.[/bold red]")
            return True
        return False

    def do_target(self, arg):
        """Select a group context: target <group_name>"""
        if not self._check_db(): return
        if self._check_lock(): return # Prevent navigation if locked
        
        name = arg.strip()
        if not name:
            console.print("[yellow]Usage: target <group_name>[/yellow]")
            return
        
        # Smart Check
        all_groups = self.db.get_all_groups()
        if name not in all_groups:
            # Fuzzy match
            import difflib
            matches = difflib.get_close_matches(name, all_groups, n=1, cutoff=0.6)
            if matches:
                suggestion = matches[0]
                if console.input(f"[yellow]Group '{name}' not found. Did you mean '{suggestion}'? (y/n): [/yellow]").lower() == 'y':
                    name = suggestion
                else:
                    # Create new?
                    if console.input(f"[blue]Create new group '{name}'? (y/n): [/blue]").lower() != 'y':
                        return
            else:
                if console.input(f"[blue]Group '{name}' not found. Create new? (y/n): [/blue]").lower() != 'y':
                    return

        # Save history before switching
        if self.current_group:
            self.prev_group = self.current_group

        self.current_group = self.db.group(name)
        self.current_doc = None # Reset doc selection when changing group
        self._update_prompt()
        console.print(f"[green]Target locked: [bold]{name}[/bold][/green]")

    def do_jump(self, arg):
        """Switch back to the previous group (Like 'cd -')."""
        if not self._check_db(): return
        if self._check_lock(): return

        if not self.prev_group:
            console.print("[yellow]No previous group to jump to.[/yellow]")
            return

        # Swap current and prev
        current_name = self.current_group.name if self.current_group else None
        target_group = self.prev_group
        
        # Validation in case prev group was deleted
        if target_group.name not in self.db.get_all_groups():
             console.print(f"[red]Previous group '{target_group.name}' no longer exists.[/red]")
             self.prev_group = None
             return

        self.current_group = target_group
        # Set new prev to the one we just left
        if current_name:
             self.prev_group = self.db.group(current_name)
        
        self.current_doc = None
        self._update_prompt()
        console.print(f"[green]Jumped to: [bold]{self.current_group.name}[/bold][/green]")

    def do_cancel(self, arg):
        """
        Cancel current selection (Go back one level).
        """
        if self._check_lock(): return # Prevent cancel if locked
        
        if self.current_doc:
            console.print(f"[yellow]Unlocking document {self.current_doc.get('_id', '')[:6]}...[/yellow]")
            self.current_doc = None
        elif self.current_group:
            console.print(f"[yellow]Leaving group '{self.current_group.name}'...[/yellow]")
            self.current_group = None
        else:
            console.print("[dim]Already at root level.[/dim]")
            
        self._update_prompt()

    def do_back(self, arg):
        """Alias for cancel."""
        self.do_cancel(arg)

    def do_peek(self, arg):
        """
        View documents.
        Usage: 
          peek [limit]        : View first N docs (Default: 20)
          peek full           : View ALL docs without truncation
          peek @<index>       : View full detail of specific doc by index
        """
        if not self.current_group:
            console.print("[red]No group selected. Use 'target <group>' first.[/red]")
            return

        limit = 20
        show_full = False
        target_idx = None

        arg = arg.strip()
        if arg:
            if arg == "full":
                show_full = True
                limit = 1000000 # No limit effectively
            elif arg.startswith("@"):
                try:
                    target_idx = int(arg[1:])
                except: 
                    console.print("[red]Invalid index format. Use @0, @1...[/red]")
                    return
            elif arg.isdigit():
                limit = int(arg)
            else:
                # Handle edge case "peek 50 full" -> logic parsing manually
                parts = arg.split()
                if "full" in parts:
                    show_full = True
                    for p in parts:
                        if p.isdigit(): limit = int(p)
                else:
                    console.print(f"[yellow]Unknown argument: {arg}. Using default limit.[/yellow]")

        # Fetch Data
        group_data = self.current_group.storage.data["groups"][self.current_group.name]
        total_docs = len(group_data)
        
        # Mode 1: Single Doc View
        if target_idx is not None:
            if 0 <= target_idx < total_docs:
                doc = list(group_data.values())[target_idx]
                from rich.json import JSON
                console.print(Panel(JSON(json.dumps(doc, default=str)), title=f"[bold green]Document @{target_idx} ({doc['_id']})[/bold green]"))
            else:
                console.print(f"[red]Index @{target_idx} out of range (0-{total_docs-1}).[/red]")
            return

        # Mode 2: List View
        import itertools
        docs = list(itertools.islice(group_data.values(), limit))
        self.last_search_results = docs
        
        if not docs:
            console.print("[dim]Group is empty.[/dim]")
            return

        self._print_table(docs, full=show_full)
        
        if total_docs > limit and not show_full:
            remaining = total_docs - limit
            console.print(f"[dim]... and {remaining} more documents. Use 'peek {limit + 20}' or 'peek full' to see more.[/dim]")

    def do_hunt(self, arg):
        """
        Search documents: hunt key=value
        Supports Regex values (auto-detected if starts with 'r:')
        Example: hunt name=r:^Alice.*
        """
        if not self.current_group:
            console.print("[red]No group selected.[/red]")
            return
            
        query = self._parse_kv(arg)
        if not query: return
        
        # Advanced Find with Regex Support
        import re
        results = []
        
        # Pre-compile regexes
        regex_filters = {}
        simple_filters = {}
        
        for k, v in query.items():
            if isinstance(v, str) and v.startswith("r:"):
                try:
                    pattern = v[2:]
                    regex_filters[k] = re.compile(pattern)
                except re.error as e:
                    console.print(f"[red]Invalid Regex for '{k}': {e}[/red]")
                    return
            else:
                simple_filters[k] = v
                
        # Scan
        group_data = self.current_group.storage.data["groups"][self.current_group.name]
        for doc in group_data.values():
            match = True
            
            # 1. Simple Checks
            for k, v in simple_filters.items():
                if doc.get(k) != v:
                    match = False
                    break
            if not match: continue
            
            # 2. Regex Checks
            for k, pattern in regex_filters.items():
                val = str(doc.get(k, ""))
                if not pattern.search(val):
                    match = False
                    break
            
            if match:
                results.append(doc)

        self.last_search_results = results
        
        if not results:
            console.print("[yellow]No matches found.[/yellow]")
            return

        # Duplicate Analysis
        from collections import defaultdict
        content_map = defaultdict(list)
        for i, doc in enumerate(results):
            # Create a hashable signature of content (excluding _id and timestamps)
            sig_doc = doc.copy()
            sig_doc.pop("_id", None)
            sig_doc.pop("_created_at", None)
            sig_doc.pop("_updated_at", None)
            sig = json.dumps(sig_doc, sort_keys=True)
            content_map[sig].append(i)
            
        # If duplicates found, show summary
        has_dupes = any(len(idxs) > 1 for idxs in content_map.values())
        
        self._print_table(results)
        
        if has_dupes:
            console.print("\n[bold yellow]Duplicate Analysis:[/bold yellow]")
            for sig, idxs in content_map.items():
                if len(idxs) > 1:
                    preview = sig[:60] + "..." if len(sig) > 60 else sig
                    console.print(f"  • Content '{preview}' found at indices: [cyan]{idxs}[/cyan]")

        console.print(f"[green]Found {len(results)} matches.[/green]")

    def _print_table(self, docs, full=False):
        table = Table(show_header=True, header_style="bold magenta", box=None, show_lines=True)
        table.add_column("#", style="dim", width=4)
        table.add_column("ID", style="cyan", width=12)
        table.add_column("Data Preview", style="white", no_wrap=not full)

        for idx, doc in enumerate(docs):
            # Format data preview nicely
            data_copy = doc.copy()
            data_copy.pop("_id", None)
            data_preview = json.dumps(data_copy, default=str)
            
            if not full and len(data_preview) > 60:
                data_preview = data_preview[:57] + "..."
            
            table.add_row(str(idx), doc["_id"][:8], data_preview)
        console.print(table)

    def do_help(self, arg):
        """Show help for commands."""
        if not arg:
            # Show General Help (Rich Panel)
            self.preloop()
            return

        # Show Specific Command Help
        doc = getattr(self, f"do_{arg}", None).__doc__
        if doc:
            console.print(Panel(doc, title=f"[bold cyan]Help: {arg}[/bold cyan]", border_style="cyan"))
        else:
            console.print(f"[red]No help found for '{arg}'.[/red]")

    def do_make(self, arg):
        """
        Create a new document OR a new group.
        
        Usage:
          1. Create Document: make key=value OR make {"key": "value"}
             (Requires target group selected)
          
          2. Create Group:    make group:<name>
             Example: make group:products
             
          3. Interactive:     make (no args)
        """
        if not self._check_db(): return

        # Check for Group Creation Syntax: make group:name
        if arg.startswith("group:"):
            g_name = arg.split(":", 1)[1].strip()
            if not g_name:
                console.print("[red]Missing group name.[/red]")
                return
            
            if g_name in self.db.get_all_groups():
                console.print(f"[yellow]Group '{g_name}' already exists.[/yellow]")
                return
                
            self.db.group(g_name) # Create group
            self.db.commit()
            console.print(f"[green]Group '{g_name}' created successfully.[/green]")
            return

        # Document Creation Logic
        if not self.current_group:
            console.print("[red]No group selected. Use 'target <group>' first.[/red]")
            return
            
        data = {}
        if not arg:
            # Interactive Mode
            console.print("[cyan]Interactive Document Creation (Empty key to finish)[/cyan]")
            while True:
                key = input("  Key: ").strip()
                if not key: break
                val = input(f"  Value for '{key}': ").strip()
                
                # Auto-type conversion
                if val.isdigit(): val = int(val)
                elif val.lower() == 'true': val = True
                elif val.lower() == 'false': val = False
                
                data[key] = val
                
            if not data:
                console.print("[yellow]Creation cancelled (Empty data).[/yellow]")
                return
        else:
            data = self._parse_kv(arg)
            
        if not data: 
            console.print("[red]Invalid data format. Use key=value or JSON.[/red]")
            return
            
        res = self.current_group.insert(data)
        self.db.commit()
        console.print(f"[green]Document created. ID: {res['_id']}[/green]")

    def do_move(self, arg):
        """
        Move a document to another group.
        
        Usage: 
          1. Context Mode (Targeted):
             - move <target_group>        (Moves currently selected doc)
             - move <doc_id> <target_group>
             
          2. Global Mode (No Target):
             - move <source_group>:<doc_id> <target_group>
        """
        if not self._check_db(): return

        args = arg.split()
        source_group = self.current_group
        target_group_name = None
        doc_id = None

        # Case 1: Global Mode (source:id target)
        if ":" in args[0] and not self.current_group:
            if len(args) != 2:
                console.print("[yellow]Usage: move <source_group>:<doc_id> <target_group>[/yellow]")
                return
            
            src_str, target_group_name = args
            src_name, doc_id = src_str.split(":", 1)
            
            if src_name not in self.db.get_all_groups():
                console.print(f"[red]Source group '{src_name}' not found.[/red]")
                return
            source_group = self.db.group(src_name)
            
        # Case 2: Context Mode
        elif self.current_group:
            if len(args) == 1:
                # move <target> (uses current_doc)
                if not self.current_doc:
                    console.print("[yellow]No document selected. Use 'pick' first or 'move <id> <group>'.[/yellow]")
                    return
                doc_id = self.current_doc["_id"]
                target_group_name = args[0]
            elif len(args) == 2:
                # move <id> <target>
                doc_id, target_group_name = args
            else:
                self.do_help("move")
                return
        else:
            console.print("[red]No group selected. Use 'target' or syntax 'move <group>:<id> <target>'.[/red]")
            return

        # Execute Move
        self._exec_move_copy(source_group, doc_id, target_group_name, is_move=True)

    def do_copy(self, arg):
        """
        Copy a document to another group.
        
        Usage: 
          1. Context Mode (Targeted):
             - copy <target_group>        (Copies currently selected doc)
             - copy <doc_id> <target_group>
             
          2. Global Mode (No Target):
             - copy <source_group>:<doc_id> <target_group>
        """
        if not self._check_db(): return

        args = arg.split()
        source_group = self.current_group
        target_group_name = None
        doc_id = None

        # Case 1: Global Mode (source:id target)
        if ":" in args[0] and not self.current_group:
            if len(args) != 2:
                console.print("[yellow]Usage: copy <source_group>:<doc_id> <target_group>[/yellow]")
                return
            
            src_str, target_group_name = args
            src_name, doc_id = src_str.split(":", 1)
            
            if src_name not in self.db.get_all_groups():
                console.print(f"[red]Source group '{src_name}' not found.[/red]")
                return
            source_group = self.db.group(src_name)
            
        # Case 2: Context Mode
        elif self.current_group:
            if len(args) == 1:
                if not self.current_doc:
                    console.print("[yellow]No document selected.[/yellow]")
                    return
                doc_id = self.current_doc["_id"]
                target_group_name = args[0]
            elif len(args) == 2:
                doc_id, target_group_name = args
            else:
                self.do_help("copy")
                return
        else:
            console.print("[red]No group selected. Use 'target' or syntax 'copy <group>:<id> <target>'.[/red]")
            return

        # Execute Copy
        self._exec_move_copy(source_group, doc_id, target_group_name, is_move=False)

    def do_become(self, arg):
        """
        Switch current user context (Impersonation).
        
        Usage: become <username> [password]
        """
        if not self._check_db(): return
        
        args = arg.split()
        if not args:
            console.print("[yellow]Usage: become <username> [password][/yellow]")
            return
            
        target_user = args[0]
        password = args[1] if len(args) > 1 else None
        
        # Check if user exists
        if "users" not in self.db.storage.data or target_user not in self.db.storage.data["users"]:
             console.print(f"[red]User '{target_user}' not found.[/red]")
             return

        # Auth Logic
        # 1. If current user is Admin/Root -> Allow without password (GOD MODE)
        current = getattr(self.db, 'current_user', None)
        is_admin = False
        if current:
            user_data = self.db.storage.data["users"].get(current)
            if user_data and user_data.get("role") == "admin":
                is_admin = True
        
        # 2. If no current user (fresh shell), treat as 'su' -> Require password unless root default?
        # For debug convenience: If password provided, verify. If not, only allow if admin.
        
        if is_admin:
            self.db.current_user = target_user
            console.print(f"[green]Switched to user: [bold]{target_user}[/bold] (Admin Override)[/green]")
        else:
            if not password:
                password = console.input(f"Password for {target_user}: ", password=True)
                
            if self.db.authenticate(target_user, password):
                 console.print(f"[green]Switched to user: [bold]{target_user}[/bold][/green]")
            else:
                 console.print("[red]Authentication failed.[/red]")
                 return

        self._update_prompt()

    def do_user(self, arg):
        """
        Manage users.
        
        Usage:
          user list
          user create <name> [password] [role]
          user drop <name>
        """
        if not self._check_db(): return
        
        # Ensure perms plugin is loaded
        if 'perms' not in self.db.plugins:
             # Try to load fallback if not loaded
             try:
                 from hvpdb_perms import PermissionManager # type: ignore
                 self.db.plugins['perms'] = PermissionManager(self.db)
             except ImportError:
                 console.print("[red]Error: 'hvpdb-perms' plugin not found.[/red]")
                 return

        pm = self.db.plugins['perms']
        args = arg.split()
        if not args:
            self.do_help("user")
            return
            
        cmd = args[0].lower()
        
        if cmd == "list":
            users = pm.list_users()
            table = Table(title="Database Users")
            table.add_column("Username", style="cyan")
            table.add_column("Role", style="magenta")
            table.add_column("Groups", style="green")
            
            for u, data in users.items():
                groups = ", ".join(data.get("groups", []))
                table.add_row(u, data.get("role"), groups)
            console.print(table)
            
        elif cmd == "create":
            if len(args) < 2:
                console.print("[yellow]Usage: user create <username> [password] [role][/yellow]")
                return
            
            username = args[1]
            password = args[2] if len(args) > 2 else None
            role = args[3] if len(args) > 3 else "user"
            
            if not password:
                password = console.input(f"Enter password for '{username}': ", password=True)
            
            try:
                pm.create_user(username, password, role)
                self.db.commit()
                console.print(f"[green]User '{username}' created.[/green]")
            except Exception as e:
                console.print(f"[red]Error: {e}[/red]")
                
        elif cmd == "drop":
            if len(args) < 2:
                 console.print("[yellow]Usage: user drop <username>[/yellow]")
                 return
            
            username = args[1]
            if console.input(f"Are you sure you want to delete user '{username}'? (y/n) ").lower() != 'y':
                return
                
            try:
                # Manual delete since perms plugin might not have delete yet? 
                # Checking perms plugin interface... usually it has.
                # If not, direct delete from storage
                if username in self.db.storage.data["users"]:
                    del self.db.storage.data["users"][username]
                    self.db.storage._dirty = True
                    self.db.commit()
                    console.print(f"[green]User '{username}' deleted.[/green]")
                else:
                    console.print(f"[red]User '{username}' not found.[/red]")
            except Exception as e:
                console.print(f"[red]Error: {e}[/red]")
        else:
            console.print(f"[red]Unknown user command: {cmd}[/red]")

    def do_grant(self, arg):
        """
        Grant permission to a user.
        Usage: grant <username> <group>
        """
        if not self._check_db(): return
        
        args = arg.split()
        if len(args) != 2:
            console.print("[yellow]Usage: grant <username> <group>[/yellow]")
            return
            
        username, group = args
        
        if 'perms' not in self.db.plugins:
             console.print("[red]Permissions plugin not loaded.[/red]")
             return
             
        try:
            self.db.plugins['perms'].grant(username, group)
            self.db.commit()
            console.print(f"[green]Granted access to '{group}' for '{username}'.[/green]")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")

    def do_revoke(self, arg):
        """
        Revoke permission from a user.
        Usage: revoke <username> <group>
        """
        if not self._check_db(): return
        
        args = arg.split()
        if len(args) != 2:
            console.print("[yellow]Usage: revoke <username> <group>[/yellow]")
            return
            
        username, group = args
        
        if 'perms' not in self.db.plugins:
             console.print("[red]Permissions plugin not loaded.[/red]")
             return
             
        try:
            self.db.plugins['perms'].revoke(username, group)
            self.db.commit()
            console.print(f"[green]Revoked access to '{group}' from '{username}'.[/green]")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")

    def _exec_move_copy(self, source_group, doc_id, target_group_name, is_move):
        """Helper to execute move/copy logic."""
        if target_group_name not in self.db.get_all_groups():
            console.print(f"[red]Target group '{target_group_name}' not found.[/red]")
            return

        if source_group.name == target_group_name:
            console.print("[yellow]Source and target groups are the same.[/yellow]")
            return

        doc = source_group.find_one({"_id": doc_id})
        if not doc:
            console.print(f"[red]Document {doc_id} not found in '{source_group.name}'.[/red]")
            return

        try:
            # Prepare new doc
            import copy
            new_doc = copy.deepcopy(doc)
            
            # For Copy: Generate new ID to avoid conflict? 
            # Logic: If move, we keep ID (usually). If copy, we might want new ID.
            # But HVPDB insert() handles ID generation if missing.
            # Let's keep ID for move (preserve identity), generate new for copy (clone).
            if not is_move:
                if "_id" in new_doc: del new_doc["_id"]
            
            # Insert to target
            res = self.db.group(target_group_name).insert(new_doc)
            
            # If Move: Delete from source
            if is_move:
                source_group.delete({"_id": doc_id})
                msg_action = "Moved"
                
                # Reset context if we moved the currently selected doc
                if self.current_doc and self.current_doc.get("_id") == doc_id:
                    self.current_doc = None
                    self._update_prompt()
            else:
                msg_action = "Copied"

            self.db.commit()
            console.print(f"[green]{msg_action} document to '{target_group_name}'. New ID: {res['_id'][:8]}[/green]")

        except Exception as e:
            console.print(f"[red]Operation failed: {e}[/red]")

    def do_random(self, arg):
        """Pick a random document."""
        if not self.current_group:
            console.print("[red]No group selected.[/red]")
            return
            
        import random
        docs = self.current_group.find()
        if not docs:
            console.print("[dim]Group is empty.[/dim]")
            return
            
        doc = random.choice(docs)
        self.current_doc = doc
        self._update_prompt()
        
        from rich.json import JSON
        json_str = json.dumps(doc, indent=2, default=str)
        console.print(Panel(JSON(json_str), title="[bold green]Random Pick (LOCKED)[/bold green]", border_style="green"))

    def do_fields(self, arg):
        """Show all unique fields in current group."""
        if not self.current_group:
            console.print("[red]No group selected.[/red]")
            return
            
        fields = set()
        for doc in self.current_group.find():
            fields.update(doc.keys())
            
        console.print(Panel("\n".join(sorted(fields)), title=f"Fields in {self.current_group.name}"))

    def do_nuke(self, arg):
        """
        Delete a group permanently.
        
        Usage: nuke <group_name>
        Alias: drop
        """
        if not self._check_db(): return
        if not arg:
            self.do_help("nuke")
            return
            
        confirm = input(f"🔥 WARNING: Nuke group '{arg}'? (y/n): ")
        if confirm.lower() == 'y':
            if arg in self.db.storage.data["groups"]:
                del self.db.storage.data["groups"][arg]
                self.db.storage._dirty = True
                self.db.commit()
                console.print(f"[bold red]💥 Group '{arg}' nuked.[/bold red]")
                
                if self.current_group and self.current_group.name == arg:
                    self.current_group = None
                    self.prompt = "hvpdb > "
            else:
                console.print(f"[yellow]Group '{arg}' not found.[/yellow]")

    def do_drop(self, arg):
        """Alias for nuke."""
        self.do_nuke(arg)

    def do_version(self, arg):
        """Show HVPDB version."""
        from . import __version__ as pkg_version
        console.print(f"[bold cyan]HVPDB v{pkg_version}[/bold cyan]")
        console.print("Engine: HVP-Storage (Python)")

    def do_config(self, arg):
        """
        Manage Shell Configuration.
        
        Usage:
          config set <key> <value>
          config get <key>
          config list
        """
        # Placeholder for config logic
        console.print("[dim]Config management coming soon...[/dim]")

    def do_backup(self, arg):
        """
        Backup database to file.
        
        Usage: backup <filename.hvp>
        """
        if not arg:
            self.do_help("backup")
            return
            
        import shutil
        try:
            if hasattr(self.db, 'filepath') and os.path.exists(self.db.filepath):
                 shutil.copy2(self.db.filepath, arg)
                 console.print(f"[green]Backup created at {arg}[/green]")
            else:
                 console.print("[red]Cannot backup in-memory or cluster DB yet.[/red]")
        except Exception as e:
            console.print(f"[red]Backup failed: {e}[/red]")

    def do_pick(self, arg):
        """Select document by index from last search results."""
        if not self.last_search_results:
            console.print("[yellow]No results to pick from. Run 'peek' or 'hunt' first.[/yellow]")
            return
            
        try:
            idx = int(arg)
            if 0 <= idx < len(self.last_search_results):
                self.current_doc = self.last_search_results[idx]
                self._update_prompt()
                
                # Use Rich JSON for beautiful display
                from rich.json import JSON
                json_str = json.dumps(self.current_doc, indent=2, default=str)
                console.print(Panel(JSON(json_str), title="[bold green]Selected Document (LOCKED)[/bold green]", border_style="green"))
            else:
                console.print("[red]Index out of range.[/red]")
        except ValueError:
            console.print("[red]Invalid index.[/red]")

    def do_morph(self, arg):
        """Update selected document: morph key=value"""
        if not self.current_doc:
            console.print("[red]No document selected.[/red]")
            return
            
        updates = self._parse_kv(arg)
        if not updates: return
            
        self.current_group.update({"_id": self.current_doc["_id"]}, updates)
        self.db.commit()
        
        self.current_doc.update(updates)
        console.print("[green]Document updated successfully.[/green]")

    def do_throw(self, arg):
        """Delete selected document."""
        if not self.current_doc:
            console.print("[red]No document selected.[/red]")
            return
            
        confirm = input(f"Delete document {self.current_doc['_id']}? (y/n): ")
        if confirm.lower() == 'y':
            self.current_group.delete({"_id": self.current_doc["_id"]})
            self.db.commit()
            console.print("[green]Document deleted.[/green]")
            
            self.current_doc = None
            self.prompt = f"hvpdb:{self.current_group.name} > "

    def do_check(self, arg):
        """Count documents in current group."""
        if not self.current_group:
            console.print("[red]No group selected.[/red]")
            return
        count = self.current_group.count()
        console.print(f"Total documents: {count}")

    def do_truncate(self, arg):
        """Delete ALL documents in current group."""
        if not self.current_group:
            console.print("[red]No group selected.[/red]")
            return
            
        confirm = input(f"WARNING: Delete ALL data in '{self.current_group.name}'? (yes/no): ")
        if confirm.lower() == 'yes':
            # Efficient truncate: delete file in cluster mode, or clear dict in single mode
            if hasattr(self.db, 'is_cluster') and self.db.is_cluster:
                 # TODO: Implement cluster truncate
                 console.print("[yellow]Cluster truncate not optimized yet. Using slow delete.[/yellow]")
                 all_docs = self.current_group.find()
                 for d in all_docs:
                     self.current_group.delete({"_id": d["_id"]})
            else:
                 # Fast path for single file
                 self.db.storage.data["groups"][self.current_group.name] = {}
                 self.db.storage._dirty = True
                 
            self.db.commit()
            console.print(f"[green]Group '{self.current_group.name}' truncated.[/green]")

    def do_index(self, arg):
        """
        Create an index on a field.
        Usage: index <field> [unique]
        Example: 
          index email
          index username unique
        """
        if not self.current_group:
            console.print("[red]No group selected.[/red]")
            return
            
        args = arg.split()
        if not args:
            console.print("[yellow]Usage: index <field> [unique][/yellow]")
            return
            
        field = args[0]
        unique = False
        if len(args) > 1 and args[1].lower() == "unique":
            unique = True
            
        try:
            self.current_group.create_index(field, unique=unique)
            self.db.commit()
            type_str = "UNIQUE" if unique else "STANDARD"
            console.print(f"[green]{type_str} Index created on '{field}'.[/green]")
        except ValueError as e:
            console.print(f"[red]Failed to create index: {e}[/red]")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")

    def do_export(self, arg):
        """Export group data to JSON: export data.json"""
        if not self.current_group:
            console.print("[red]No group selected.[/red]")
            return
        if not arg:
            console.print("[yellow]Usage: export <filename.json>[/yellow]")
            return
            
        docs = self.current_group.find()
        try:
            with open(arg, 'w', encoding='utf-8') as f:
                json.dump(docs, f, indent=2, default=str)
            console.print(f"[green]Exported {len(docs)} documents to {arg}[/green]")
        except Exception as e:
            console.print(f"[red]Export failed: {e}[/red]")

    def do_import(self, arg):
        """Import JSON data: import data.json"""
        if not self.current_group:
            console.print("[red]No group selected.[/red]")
            return
        if not arg:
            console.print("[yellow]Usage: import <filename.json>[/yellow]")
            return
        
        if not os.path.exists(arg):
            console.print(f"[red]File not found: {arg}[/red]")
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
                console.print(f"[green]Imported {count} documents.[/green]")
            else:
                console.print("[red]Invalid JSON: Expected a list of objects.[/red]")
        except Exception as e:
            console.print(f"[red]Import failed: {e}[/red]")

    def do_trace(self, arg):
        """View audit log for selected document."""
        if not self.current_doc:
            console.print("[red]Select a document first.[/red]")
            return
            
        if not hasattr(self.current_group, 'get_audit_trail'):
             console.print("[yellow]Audit logging not available.[/yellow]")
             return
             
        logs = self.current_group.get_audit_trail(self.current_doc["_id"])
        if not logs:
            console.print("[dim]No history found.[/dim]")
            return
            
        table = Table(title=f"Audit Log: {self.current_doc['_id'][:8]}")
        table.add_column("Time", style="dim")
        table.add_column("Action", style="magenta")
        table.add_column("Data", style="white")
        
        import datetime
        for log in logs:
            ts = datetime.datetime.fromtimestamp(log.get("timestamp", 0)).strftime('%Y-%m-%d %H:%M:%S')
            op = log.get("op", "unknown")
            data = str(log.get("data", {}))[:60]
            table.add_row(ts, op, data)
        console.print(table)

    def do_status(self, arg):
        """Show database status."""
        if not self._check_db(): return
        size_mb = 0
        if hasattr(self.db, 'filepath') and os.path.exists(self.db.filepath):
             size_mb = os.path.getsize(self.db.filepath) / (1024 * 1024)
        
        console.print(Panel(f"""
        [bold]Database Status[/bold]
        ----------------
        Path: {self.db.filepath}
        Size: {size_mb:.2f} MB
        Encrypted: {'Yes' if self.db.password else 'No'}
        Groups: {len(self.db.get_all_groups())}
        """, title="Info"))

    def do_save(self, arg):
        """
        Save changes to disk immediately.
        Usage: 
          save              : Save now
          save auto [on/off]: Toggle auto-save on exit
        """
        if not self._check_db(): return
        
        args = arg.split()
        if args and args[0] == "auto":
            if len(args) > 1:
                mode = args[1].lower()
                self.auto_save = (mode == "on")
            console.print(f"Auto-Save on Exit: [{'green' if self.auto_save else 'yellow'}]{'ON' if self.auto_save else 'OFF (Ask)'}[/]")
            return

        self.db.commit()
        console.print("[green]Database saved successfully.[/green]")

    def do_quit(self, arg):
        """Exit the shell."""
        if self._check_lock(): return 
        
        if self.db:
            # Check if dirty
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
                    console.print("[dim]Auto-saving...[/dim]")
                    self.db.commit()
                else:
                    ans = console.input("[yellow]Unsaved changes detected. Save before exit? (y/n/cancel): [/yellow]").lower()
                    if ans == 'y':
                        self.db.commit()
                        console.print("[green]Saved.[/green]")
                    elif ans == 'n':
                        console.print("[red]Changes discarded.[/red]")
                    else:
                        console.print("[dim]Cancelled exit.[/dim]")
                        return False

            try:
                self.db.close()
            except Exception as e:
                console.print(f"[red]Error closing DB: {e}[/red]")
                
        console.print("[bold cyan]Bye! 👋[/bold cyan]")
        return True

    def do_tree(self, arg):
        """
        Visualize database structure as a tree.
        Usage: tree
        """
        if not self._check_db(): return
        
        tree = Tree(f"[bold cyan]📦 {os.path.basename(self.db.filepath)}[/bold cyan]")
        
        groups = self.db.get_all_groups()
        for g_name in groups:
            grp = self.db.group(g_name)
            count = grp.count()
            
            # Group Node
            g_node = tree.add(f"[yellow]📂 {g_name}[/yellow] [dim]({count} docs)[/dim]")
            
            # Show indexes if any
            if hasattr(grp, 'indexes') and grp.indexes:
                idx_node = g_node.add("[dim]Indexes[/dim]")
                for field in grp.indexes:
                    idx_node.add(f"🔑 {field}")
            
            # Show unique indexes
            if hasattr(grp, 'unique_indexes') and grp.unique_indexes:
                uidx_node = g_node.add("[dim]Unique Constraints[/dim]")
                for field in grp.unique_indexes:
                    uidx_node.add(f"🔒 {field}")

        console.print(tree)

    def do_validate(self, arg):
        """
        Check database integrity.
        Usage: validate
        """
        if not self._check_db(): return
        
        console.print("[bold]Running Integrity Check...[/bold]")
        issues = 0
        
        for g_name in self.db.get_all_groups():
            grp = self.db.group(g_name)
            docs = grp.find()
            console.print(f"Checking group '{g_name}' ({len(docs)} docs)...", end="")
            
            g_issues = 0
            for doc in docs:
                # 1. Check ID
                if "_id" not in doc:
                    console.print(f"\n  [red]CRITICAL: Doc missing _id: {str(doc)[:50]}...[/red]")
                    g_issues += 1
                
                # 2. Check Serialization (Simulated)
                try:
                    json.dumps(doc)
                except Exception as e:
                     console.print(f"\n  [red]ERROR: Doc {doc.get('_id')} is not JSON serializable: {e}[/red]")
                     g_issues += 1
            
            if g_issues == 0:
                console.print(" [green]OK[/green]")
            else:
                issues += g_issues
                
        if issues == 0:
            console.print("\n[bold green]✅ Database is HEALTHY.[/bold green]")
        else:
            console.print(f"\n[bold red]❌ Found {issues} issues.[/bold red]")

    def do_monitor(self, arg):
        """
        Monitor database status (Auto-refresh).
        Usage: monitor [interval_sec]
        """
        if not self._check_db(): return
        import time
        
        interval = 2
        if arg and arg.isdigit():
            interval = int(arg)
            
        console.print(f"[cyan]Monitoring... (Ctrl+C to stop)[/cyan]")
        try:
            with console.status("Monitoring DB Activity...") as status:
                while True:
                    # Collect stats
                    total_docs = 0
                    groups = self.db.get_all_groups()
                    for g in groups:
                        total_docs += self.db.group(g).count()
                    
                    size_mb = os.path.getsize(self.db.filepath) / (1024*1024) if os.path.exists(self.db.filepath) else 0
                    
                    status.update(f"Groups: {len(groups)} | Docs: {total_docs} | Size: {size_mb:.2f} MB")
                    time.sleep(interval)
        except KeyboardInterrupt:
            console.print("\n[dim]Monitor stopped.[/dim]")

    def do_record(self, arg):
        """
        Data Version Control System.
        
        Usage:
          record list [limit]      : Show recent transactions
          record peek <seq>        : Show changes (+/-)
          record undo <seq>        : Revert a transaction
          record apply <seq>       : Re-apply a transaction
          record status [on/off]   : Toggle recording visualization
        """
        if not self._check_db(): return
        
        args = arg.split()
        if not args:
            self.do_help("record")
            return
            
        cmd = args[0].lower()
        
        if cmd == "status":
            if len(args) > 1:
                mode = args[1].lower()
                self.record_mode = (mode == "on")
            console.print(f"Record Mode: [{'green' if self.record_mode else 'red'}]{'ON' if self.record_mode else 'OFF'}[/]")
            
        elif cmd == "list":
            limit = int(args[1]) if len(args) > 1 and args[1].isdigit() else 10
            # Fetch global logs from storage WAL
            if hasattr(self.db.storage, 'wal'):
                logs = []
                def collector(entry):
                    logs.append(entry)
                
                self.db.storage.wal.replay(0, collector)
                logs = sorted(logs, key=lambda x: x.get("seq", 0), reverse=True)[:limit]
                
                table = Table(title=f"Transaction History (Last {limit})")
                table.add_column("Seq", style="cyan", width=6)
                table.add_column("Txn ID", style="blue", width=8)
                table.add_column("Time", style="dim")
                table.add_column("Op", style="magenta")
                table.add_column("Group", style="yellow")
                table.add_column("ID", style="white")
                
                import datetime
                for log in logs:
                    ts = datetime.datetime.fromtimestamp(log.get("ts", 0)).strftime('%H:%M:%S')
                    txn = log.get("txn", "")[:8] if log.get("txn") else "-"
                    table.add_row(
                        str(log.get("seq")),
                        txn,
                        ts, 
                        log.get("op"), 
                        log.get("g"), 
                        str(log.get("id"))[:8]
                    )
                console.print(table)
            else:
                console.print("[red]WAL not accessible.[/red]")

        elif cmd == "peek":
            if len(args) < 2:
                console.print("[yellow]Usage: record peek <seq>[/yellow]")
                return
            
            target_seq = int(args[1])
            found_log = None
            def finder(entry):
                nonlocal found_log
                if entry.get("seq") == target_seq:
                    found_log = entry
            
            self.db.storage.wal.replay(0, finder)
            
            if not found_log:
                console.print(f"[red]Record #{target_seq} not found.[/red]")
                return
                
            # Show Diff
            data = found_log.get("d")
            before = found_log.get("b")
            op = found_log.get("op")
            
            console.print(Panel(f"Transaction #{target_seq} - {op.upper()}", style="blue"))
            if op == "insert":
                console.print(f"[green]+ {json.dumps(data, indent=2)}[/green]")
            elif op == "delete":
                console.print(f"[red]- {json.dumps(data, indent=2)}[/red]")
            elif op == "update":
                if before:
                    console.print("[red]Before:[/red]")
                    console.print(f"[dim]{json.dumps(before, indent=2)}[/dim]")
                    console.print("[green]After:[/green]")
                    console.print(f"{json.dumps(data, indent=2)}")
                else:
                    console.print(f"[yellow]~ {json.dumps(data, indent=2)}[/yellow]")
                    console.print("[dim](Old value not available in log)[/dim]")

        elif cmd == "undo":
            if len(args) < 2:
                console.print("[yellow]Usage: record undo <seq>[/yellow]")
                return
                
            seq = int(args[1])
            # Find log
            found_log = None
            def finder(entry):
                nonlocal found_log
                if entry.get("seq") == seq:
                    found_log = entry
            self.db.storage.wal.replay(0, finder)
            
            if not found_log:
                console.print(f"[red]Record #{seq} not found.[/red]")
                return
            
            # ATOMIC UNDO LOGIC
            target_txn_id = found_log.get("txn")
            if not target_txn_id:
                console.print("[red]Cannot undo legacy transaction (missing Txn ID).[/red]")
                return

            # Find ALL operations in this transaction
            txn_ops = []
            def txn_collector(entry):
                if entry.get("txn") == target_txn_id and entry.get("type") == "DATA":
                    txn_ops.append(entry)
            self.db.storage.wal.replay(0, txn_collector)
            
            # Sort by sequence descending (Undo in reverse order)
            txn_ops.sort(key=lambda x: x.get("seq"), reverse=True)
            
            console.print(f"[bold]Undoing Transaction {target_txn_id[:8]} ({len(txn_ops)} operations)...[/bold]")
            
            if console.input(f"Confirm undo? (y/n) ").lower() != 'y': return

            # Start Compensation Transaction
            undo_txn_id = self.db.storage.begin_txn()
            
            try:
                for op_log in txn_ops:
                    op = op_log.get("op")
                    grp_name = op_log.get("g")
                    doc_id = op_log.get("id")
                    data = op_log.get("d")
                    before = op_log.get("b")
                    
                    grp = self.db.group(grp_name)
                    
                    if op == "insert":
                        # Undo Insert -> Delete
                        grp.delete({"_id": doc_id}, external_txn_id=undo_txn_id)
                        console.print(f"[green]Reverted Insert: Deleted {doc_id}[/green]")
                        
                    elif op == "delete":
                        # Undo Delete -> Insert (Restore)
                        if grp.find_one({"_id": doc_id}):
                             console.print(f"[yellow]Warning: Document {doc_id} already exists. Skipping restore.[/yellow]")
                        else:
                            restore_data = before if before else data
                            grp.insert(restore_data, external_txn_id=undo_txn_id)
                            console.print(f"[green]Reverted Delete: Restored {doc_id}[/green]")
                            
                    elif op == "update":
                        if before:
                            # Undo Update -> Restore Before Image
                            grp.update({"_id": doc_id}, before, external_txn_id=undo_txn_id)
                            console.print(f"[green]Reverted Update: Restored {doc_id}[/green]")
                        else:
                            console.print(f"[red]Cannot undo update {doc_id}: Missing before-image.[/red]")
                            raise ValueError("Missing before-image")

                self.db.storage.commit_txn(undo_txn_id)
                self.db.commit() # Save snapshot
                console.print("[bold green]Transaction Undone Successfully.[/bold green]")
                
            except Exception as e:
                self.db.storage.rollback_txn(undo_txn_id)
                console.print(f"[bold red]Undo Failed: {e}. Rolled back changes.[/bold red]")

        elif cmd == "apply":
            if len(args) < 2:
                console.print("[yellow]Usage: record apply <seq>[/yellow]")
                return
                
            seq = int(args[1])
            # Find log
            found_log = None
            def finder(entry):
                nonlocal found_log
                if entry.get("seq") == seq:
                    found_log = entry
            self.db.storage.wal.replay(0, finder)
            
            if not found_log: return
            
            op = found_log.get("op")
            grp = self.db.group(found_log.get("g"))
            data = found_log.get("d")
            
            if console.input(f"Re-apply {op} #{seq}? (y/n) ").lower() != 'y': return
            
            if op == "insert":
                grp.insert(data)
            elif op == "delete":
                grp.delete({"_id": found_log.get("id")})
            elif op == "update":
                grp.update({"_id": found_log.get("id")}, data)
                
            self.db.commit()
            console.print("[green]Transaction re-applied.[/green]")
