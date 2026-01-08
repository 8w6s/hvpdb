import sys
import os
import json
import ast
from typing import Optional
import difflib

try:
    import typer
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.json import JSON
except ImportError:
    print("Error: CLI dependencies not found.")
    print("Please install with: pip install hvpdb[cli] or pip install typer rich")
    sys.exit(1)

from .core import HVPDB
from .uri import HVPURI

try:
    if sys.version_info < (3, 10):
        from importlib_metadata import entry_points
    else:
        from importlib.metadata import entry_points
except ImportError:
    entry_points = None

PLUGINS = {}

def load_plugins():
    # 1. Entry Points (Preferred)
    if entry_points:
        try:
            eps = entry_points()
            if hasattr(eps, 'select'):
                plugins = eps.select(group='hvpdb.plugins')
            else:
                plugins = eps.get('hvpdb.plugins', [])
                
            for ep in plugins:
                try:
                    PLUGINS[ep.name] = ep.load()
                except Exception as e:
                    console.print(f"[yellow]Warning: Failed to load plugin {ep.name}: {e}[/yellow]")
        except Exception:
            pass

    # 2. Hardcoded Discovery (Legacy/Dev fallback)
    if 'perms' not in PLUGINS:
        try:
            from hvpdb_perms import PermissionManager # type: ignore
            PLUGINS['perms'] = PermissionManager
        except ImportError:
            pass

load_plugins()

app = typer.Typer(help="HVPDB CLI - High Velocity Python Database", no_args_is_help=False, add_completion=False)
console = Console()

@app.callback(invoke_without_command=True)
def hvpdb_main(ctx: typer.Context):
    """
    HVPDB - High Velocity Python Database (Enterprise Edition)
    """
    if ctx.invoked_subcommand is None:
        hvpdb_show_help()

def hvpdb_get_db(uri_or_path: str, password: str = None) -> HVPDB:
    try:
        if not uri_or_path.startswith("hvp://") and not password:
             # Try to get password interactively if not provided
             # But to keep it non-blocking if possible, we just try without first
             pass
        return HVPDB(uri_or_path, password)
    except Exception as e:
        # If password failure, prompt?
        # For now, just fail to keep it simple or prompt
        if "BadDecrypt" in str(e) or "password" in str(e).lower():
             password = typer.prompt("Enter Database Password", hide_input=True)
             return HVPDB(uri_or_path, password)
        console.print(f"[bold red]Connection Error:[/bold red] {repr(e)}")
        raise typer.Exit(code=1)

@app.command(name="init", help="Initialize a new database.\n\nUsage: hvpdb init <target> [password]")
def hvpdb_init(
    target: str = typer.Argument(..., help="File path or URI"),
    password: Optional[str] = typer.Argument(None, help="Password (Optional)"),
):
    """Initialize a new HVPDB database."""
    # Smart Init: Add .hvp extension if missing
    if not target.startswith("hvp://") and not target.endswith(".hvp") and not target.endswith(".hvdb"):
        target += ".hvp"
        
    if os.path.exists(target) and not target.startswith("hvp://"):
        console.print(f"[yellow]File {target} already exists![/yellow]")
        if not typer.confirm("Do you want to overwrite it?"):
            raise typer.Exit()
            
    if not target.startswith("hvp://") and not password:
        password = typer.prompt("New Password", hide_input=True, confirmation_prompt=True)

    try:
        db = HVPDB(target, password)
        db.storage.save() 
        abs_path = os.path.abspath(db.filepath)
        console.print(Panel(
            f"[bold green]Database created successfully![/bold green]\n"
            f"[bold white]Location:[/bold white] {abs_path}\n"
            f"[dim]Note: This file is stored in your current project directory.[/dim]", 
            title="Success"
        ))
    except Exception as e:
        console.print(f"[bold red]Init Failed:[/bold red] {e}")

@app.command(name="compact", help="Compact storage.\n\nUsage: hvpdb compact <target> [password]")
def hvpdb_compact(
    target: str = typer.Argument(..., help="Database Path"),
    password: Optional[str] = typer.Argument(None, help="Password"),
):
    """Compact database storage (Rewrite file)."""
    db = hvpdb_get_db(target, password)
    console.print("[yellow]Compacting database...[/yellow]")
    db.storage._dirty = True # Force dirty
    if hasattr(db, 'is_cluster') and db.is_cluster:
        for name in db.get_all_groups():
            grp = db.group(name)
            grp.storage._dirty = True
            
    db.commit()
    console.print("[bold green]Compaction complete![/bold green]")

@app.command(name="create-group", help="Create a new group.\n\nUsage: hvpdb create-group <target> <name> [password]")
def hvpdb_create_group(
    target: str = typer.Argument(..., help="Database Path"),
    name: str = typer.Argument(..., help="Group Name"),
    password: Optional[str] = typer.Argument(None, help="Password"),
):
    """Create a new group."""
    db = hvpdb_get_db(target, password)
    if name in db.get_all_groups():
        console.print(f"[yellow]Group '{name}' already exists.[/yellow]")
        return
        
    # Trigger creation
    db.group(name)
    db.commit()
    console.print(f"[bold green]Group '{name}' created successfully.[/bold green]")

@app.command(name="drop-group")
def hvpdb_drop_group(
    target: str = typer.Argument(..., help="Database Path"),
    name: str = typer.Argument(..., help="Group Name"),
    password: Optional[str] = typer.Argument(None, help="Password"),
):
    """Delete a group and all its documents."""
    db = hvpdb_get_db(target, password)
    if name not in db.get_all_groups():
        console.print(f"[red]Group '{name}' not found.[/red]")
        return

    if not typer.confirm(f"Are you sure you want to delete group '{name}'?"):
        return
            
    # Logic to delete group
    if hasattr(db, 'is_cluster') and db.is_cluster:
        group_path = os.path.join(db.filepath, f"{name}.hvp")
        if os.path.exists(group_path):
            os.remove(group_path)
    else:
        if name in db.storage.data["groups"]:
            del db.storage.data["groups"][name]
            db.storage._dirty = True
            db.commit()
            
    console.print(f"[bold green]Group '{name}' deleted.[/bold green]")

@app.command(name="drop-db", help="Destroy the database.\n\nUsage: hvpdb drop-db <target>")
def hvpdb_drop_db(
    target: str = typer.Argument(..., help="Database Path"),
):
    """Delete the entire database."""
    if not typer.confirm(f"🔥 DANGER: Are you sure you want to DESTROY database '{target}'?"):
        return
            
    import shutil
    try:
        if os.path.isdir(target):
            shutil.rmtree(target)
        else:
            os.remove(target)
        console.print(f"[bold red]Database '{target}' destroyed.[/bold red]")
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")

@app.command(name="import")
def hvpdb_import(
    target: str = typer.Argument(..., help="Database Path"),
    file: str = typer.Argument(..., help="Input file (JSON)"),
    group: str = typer.Argument("default", help="Target Group"),
    password: Optional[str] = typer.Argument(None, help="Password"),
):
    """Import data from JSON file. Usage: hvpdb import <db> <file> [group] [password]"""
    db = hvpdb_get_db(target, password)
    
    if not os.path.exists(file):
        console.print(f"[red]File '{file}' not found.[/red]")
        return
        
    with open(file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    if isinstance(data, list):
        count = 0
        with console.status("Importing..."):
            for item in data:
                if isinstance(item, dict):
                    db.group(group).insert(item)
                    count += 1
        db.commit()
        console.print(f"[bold green]Imported {count} documents into group '{group}'.[/bold green]")
    elif isinstance(data, dict):
         pass
    else:
        console.print("[red]Invalid JSON format. Expected list of objects.[/red]")

@app.command(name="insert", help="Insert a document.\n\nUsage: hvpdb insert <target> <group> <data> [password]")
def hvpdb_insert(
    target: str = typer.Argument(..., help="File path or URI"),
    group: str = typer.Argument(..., help="Group name"),
    data: str = typer.Argument(..., help="JSON data string"),
    password: Optional[str] = typer.Argument(None, help="Password"),
):
    """Insert a document into a group."""
    db = hvpdb_get_db(target, password)
    try:
        try:
            doc = json.loads(data)
        except:
            doc = ast.literal_eval(data)
            
        if not isinstance(doc, dict):
            raise ValueError("Data must be a dictionary")
            
        res = db.group(group).insert(doc)
        db.commit()
        console.print(f"[bold green]✅ Inserted:[/bold green]")
        console.print(JSON.from_data(res))
    except Exception as e:
        console.print(f"[bold red]❌ Invalid Data:[/bold red] {e}")

@app.command(name="find", help="Find documents.\n\nUsage: hvpdb find <target> <group> [query] [limit] [password]")
def hvpdb_find(
    target: str = typer.Argument(..., help="File path or URI"),
    group: str = typer.Argument(..., help="Group name"),
    query: str = typer.Argument("{}", help="JSON query string"),
    limit: int = typer.Argument(10, help="Limit results"),
    password: Optional[str] = typer.Argument(None, help="Password"),
):
    """Find documents in a group."""
    db = hvpdb_get_db(target, password)
    try:
        try:
            q = json.loads(query)
        except:
            try:
                q = ast.literal_eval(query)
            except:
                q = {} # Default to empty if parse fails or it's just a placeholder
            
        docs = db.group(group).find(q)
        
        console.print(f"[bold cyan]🔍 Found {len(docs)} documents (Showing top {limit}):[/bold cyan]")
        for doc in docs[:limit]:
            console.print(JSON.from_data(doc))
            console.print("---")
    except Exception as e:
        console.print(f"[bold red]❌ Error:[/bold red] {e}")

@app.command(name="delete", help="Delete a document by ID.\n\nUsage: hvpdb delete <target> <group> <id> [password]")
def hvpdb_delete(
    target: str = typer.Argument(..., help="File path or URI"),
    group: str = typer.Argument(..., help="Group name"),
    id: str = typer.Argument(..., help="Document ID to delete"),
    password: Optional[str] = typer.Argument(None, help="Password"),
):
    """Delete a document by ID."""
    db = hvpdb_get_db(target, password)
    count = db.group(group).delete({"_id": id})
    db.commit()
    if count > 0:
        console.print(f"[bold green]Deleted document {id}[/bold green]")
    else:
        console.print(f"[bold yellow]Document {id} not found[/bold yellow]")

@app.command(name="passwd", help="Change password.\n\nUsage: hvpdb passwd <target> [password]")
def hvpdb_passwd(
    target: str = typer.Argument(..., help="File path or URI"),
    password: Optional[str] = typer.Argument(None, help="Current Password"),
):
    """Change database password (Re-encrypts entire DB)."""
    db = hvpdb_get_db(target, password)
    new_pass = typer.prompt("Enter New Password", hide_input=True, confirmation_prompt=True)
    
    if not new_pass:
        console.print("[red]Password cannot be empty![/red]")
        raise typer.Exit(1)
        
    db.storage.password = new_pass
    db.storage._dirty = True
    db.storage.security = None 
    
    console.print("[yellow]Re-encrypting database...[/yellow]")
    db.commit()
    console.print("[bold green]Password changed successfully![/bold green]")

@app.command(name="shell", help="Start HVPDB Ops Shell (HVPShell).\n\nUsage: hvpdb shell [target] [password] [commands]")
def hvpdb_shell(
    target: Optional[str] = typer.Argument(None, help="File path or URI"),
    password: Optional[str] = typer.Argument(None, help="Password"),
    commands: Optional[str] = typer.Argument(None, help="One-liner commands (sep by +)"),
):
    """Start the advanced HVPDB Ops Shell."""
    from .hvpshell import HVPShell
    
    shell = HVPShell()
    
    # 1. Auto Connect
    if target:
        # Construct connect command
        conn_cmd = f"connect {target}"
        if password:
            conn_cmd += f" {password}"
        shell.onecmd(conn_cmd)
        
        # Explicitly tell user where they are connected
        if shell.db:
             abs_path = os.path.abspath(shell.db.filepath)
             console.print(f"[dim]Working Directory: {os.getcwd()}[/dim]")
             console.print(f"[dim]Database Path:     {abs_path}[/dim]")
        
    # 2. Batch Mode (One-Liner)
    if commands:
        # Split by '+' (Use + instead of : to avoid JSON conflict)
        cmds = commands.split("+")
        for cmd in cmds:
            cmd = cmd.strip()
            if cmd:
                # If command is 'peek' or 'scan', ensure we see output
                shell.onecmd(cmd)
        return # Exit after execution

    # 3. Interactive Mode
    try:
        shell.cmdloop()
    except KeyboardInterrupt:
        console.print("\n[dim]Session terminated. Bye![/dim]")

@app.command(name="backup")
def hvpdb_backup(
    target: str = typer.Argument(..., help="Database Path"),
    output: str = typer.Argument("backup.hvp", help="Backup file path"),
    password: Optional[str] = typer.Argument(None, help="Password"),
):
    """Backup the database file. Usage: hvpdb backup <target> [output] [password]"""
    if os.path.isdir(target):
        console.print("[yellow]Cluster backup not yet supported (copy the folder manually).[/yellow]")
        return
        
    import shutil
    try:
        shutil.copy2(target, output)
        console.print(f"[bold green]Backup created at {output}[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Backup failed:[/bold red] {e}")

@app.command(name="stats")
def hvpdb_stats(
    target: str = typer.Argument(..., help="Database Path"),
    password: Optional[str] = typer.Argument(None, help="Password"),
):
    """Show detailed database statistics. Usage: hvpdb stats <target> [password]"""
    db = hvpdb_get_db(target, password)
    # Basic stats
    size_mb = os.path.getsize(db.filepath) / (1024 * 1024) if hasattr(db, 'filepath') and os.path.exists(db.filepath) else 0
    console.print(f"Size: {size_mb:.2f} MB")
    groups = db.get_all_groups()
    console.print(f"Groups: {len(groups)}")
    for g in groups:
        console.print(f" - {g}: {db.group(g).count()} docs")

def hvpdb_check_perms_pkg():
    if 'perms' not in PLUGINS:
        console.print("[red]Error: 'hvpdb-perms' plugin is not installed. Install it to manage users.[/red]")
        console.print("Try: [green]pip install hvpdb-perms[/green]")
        raise typer.Exit(1)

@app.command(name="create-user", help="Create a new user.\n\nUsage: hvpdb create-user <target> <username> [password] [user_password] [role]")
def hvpdb_create_user(
    target: str = typer.Argument(..., help="Database Path"),
    username: str = typer.Argument(..., help="New Username"),
    password: Optional[str] = typer.Argument(None, help="DB Password"),
    user_password: Optional[str] = typer.Argument(None, help="Password for new user"),
    role: str = typer.Argument("user", help="Role (user/admin)"),
):
    """Create a new database user."""
    hvpdb_check_perms_pkg()
    db = hvpdb_get_db(target, password)
    pm = PLUGINS['perms'](db)
    
    if not user_password:
        user_password = typer.prompt(f"Enter password for '{username}'", hide_input=True, confirmation_prompt=True)
    
    try:
        pm.create_user(username, user_password, role)
        db.commit()
        console.print(f"[bold green]User '{username}' created successfully.[/bold green]")
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")

@app.command(name="grant", help="Grant permission to user.\n\nUsage: hvpdb grant <target> <username> <group> [password]")
def hvpdb_grant(
    target: str = typer.Argument(..., help="Database Path"),
    username: str = typer.Argument(..., help="Username"),
    group: str = typer.Argument(..., help="Group to grant access to"),
    password: Optional[str] = typer.Argument(None, help="DB Password"),
):
    """Grant group access to a user."""
    hvpdb_check_perms_pkg()
    db = hvpdb_get_db(target, password)
    pm = PLUGINS['perms'](db)
    try:
        pm.grant(username, group)
        db.commit()
        console.print(f"[bold green]Granted access to '{group}' for user '{username}'.[/bold green]")
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")

@app.command(name="revoke", help="Revoke permission from user.\n\nUsage: hvpdb revoke <target> <username> <group> [password]")
def hvpdb_revoke(
    target: str = typer.Argument(..., help="Database Path"),
    username: str = typer.Argument(..., help="Username"),
    group: str = typer.Argument(..., help="Group to revoke access from"),
    password: Optional[str] = typer.Argument(None, help="DB Password"),
):
    """Revoke group access from a user."""
    hvpdb_check_perms_pkg()
    db = hvpdb_get_db(target, password)
    pm = PLUGINS['perms'](db)
    try:
        pm.revoke(username, group)
        db.commit()
        console.print(f"[bold green]Revoked access to '{group}' from user '{username}'.[/bold green]")
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")

@app.command(name="users", help="List all users.\n\nUsage: hvpdb users <target> [password]")
def hvpdb_list_users(
    target: str = typer.Argument(..., help="Database Path"),
    password: Optional[str] = typer.Argument(None, help="DB Password"),
):
    """List all users."""
    hvpdb_check_perms_pkg()
    db = hvpdb_get_db(target, password)
    pm = PLUGINS['perms'](db)
    users = pm.list_users()
    
    table = Table(title="Database Users")
    table.add_column("Username", style="cyan")
    table.add_column("Role", style="magenta")
    table.add_column("Groups", style="green")
    
    for u, data in users.items():
        groups = ", ".join(data.get("groups", []))
        table.add_row(u, data.get("role"), groups)
        
    console.print(table)

@app.command(name="export")
def hvpdb_export(
    target: str = typer.Argument(..., help="File path or URI"),
    output: str = typer.Argument("dump.json", help="Output JSON file"),
    password: Optional[str] = typer.Argument(None, help="Password"),
):
    """Export entire database to JSON. Usage: hvpdb export <target> [output] [password]"""
    db = hvpdb_get_db(target, password)
    data = db.storage.data
    with open(output, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str)
    console.print(f"[bold green]✅ Exported to {output}[/bold green]")

@app.command(name="deploy", help="Deploy HVPDB as a Network Server.\n\nUsage: hvpdb deploy <target> [port] [host] [password]")
def hvpdb_deploy(
    target: str = typer.Argument(..., help="Database Path"),
    port: int = typer.Argument(2321, help="Port to listen on"),
    host: str = typer.Argument("0.0.0.0", help="Host to bind"),
    password: Optional[str] = typer.Argument(None, help="Database Password"),
):
    """Start the HVPDB API Server."""
    from .server import start_server
    
    # Ensure DB exists or init it
    if not os.path.exists(target):
        console.print(f"[yellow]Database '{target}' does not exist. Initializing...[/yellow]")
        HVPDB(target, password).storage.save()
        
    try:
        start_server(target, password, host, port)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        console.print(f"[bold red]Server Error:[/bold red] {e}")

@app.command(name="diff", help="Compare two documents.\n\nUsage: hvpdb diff <target> <group> <id1> <id2> [password]")
def hvpdb_diff(
    target: str = typer.Argument(..., help="Database Path"),
    group: str = typer.Argument(..., help="Group Name"),
    id1: str = typer.Argument(..., help="First Document ID"),
    id2: str = typer.Argument(..., help="Second Document ID"),
    password: Optional[str] = typer.Argument(None, help="Password"),
):
    """Compare two documents."""
    db = hvpdb_get_db(target, password)
    grp = db.group(group)
    doc1 = grp.find_one({"_id": id1})
    doc2 = grp.find_one({"_id": id2})

    if not doc1:
        console.print(f"[red]Document {id1} not found.[/red]")
        return
    if not doc2:
        console.print(f"[red]Document {id2} not found.[/red]")
        return

    json1 = json.dumps(doc1, indent=2, sort_keys=True).splitlines()
    json2 = json.dumps(doc2, indent=2, sort_keys=True).splitlines()
    
    diff = difflib.unified_diff(json1, json2, fromfile=id1, tofile=id2, lineterm="")
    
    for line in diff:
        if line.startswith('+'):
            console.print(f"[green]{line}[/green]")
        elif line.startswith('-'):
            console.print(f"[red]{line}[/red]")
        elif line.startswith('^'):
            console.print(f"[blue]{line}[/blue]")
        else:
            console.print(line)

@app.command(name="jump", help="Open shell in specific group.\n\nUsage: hvpdb jump <target> <group> [password]")
def hvpdb_jump(
    target: str = typer.Argument(..., help="Database Path"),
    group: str = typer.Argument(..., help="Group to jump into"),
    password: Optional[str] = typer.Argument(None, help="Password"),
):
    """Open shell directly in a group."""
    db = hvpdb_get_db(target, password)
    if group not in db.get_all_groups():
        console.print(f"[red]Group '{group}' not found.[/red]")
        return

    from .hvpshell import HVPShell
    shell = HVPShell(db)
    shell.current_group = db.group(group)
    shell._update_prompt()
    try:
        shell.cmdloop()
    except KeyboardInterrupt:
        console.print("\n[dim]Session terminated. Bye![/dim]")

@app.command(name="dump", help="Dump search results.\n\nUsage: hvpdb dump <target> <group> [query] [output] [password]")
def hvpdb_dump(
    target: str = typer.Argument(..., help="Database Path"),
    group: str = typer.Argument(..., help="Group Name"),
    query: str = typer.Argument("{}", help="Query JSON"),
    output: str = typer.Argument("dump.json", help="Output file"),
    password: Optional[str] = typer.Argument(None, help="Password"),
):
    """Dump search results to file."""
    db = hvpdb_get_db(target, password)
    try:
        q = json.loads(query)
    except:
        q = {}
        
    docs = db.group(group).find(q)
    
    with open(output, 'w', encoding='utf-8') as f:
        json.dump(docs, f, indent=2, default=str)
        
    console.print(f"[bold green]Dumped {len(docs)} documents to {output}[/bold green]")

@app.command(name="help")
def hvpdb_help(command: Optional[str] = typer.Argument(None, help="Command to get help for")):
    """Show detailed help and usage guide."""
    if command:
        hvpdb_show_command_help(command)
    else:
        hvpdb_show_help()

def hvpdb_show_command_help(command_name: str):
    """Show specific help for a command using Rich."""
    # Find the command function
    cmd_func = None
    for cmd in app.registered_commands:
        if cmd.name == command_name or command_name in cmd.name.split(): # handle aliases if any
             cmd_func = cmd
             break
             
    if not cmd_func:
        console.print(f"[red]Command '{command_name}' not found.[/red]")
        return

    # Extract info
    help_text = cmd_func.help or "No description available."
    
    # Create Panel
    console.print(Panel(
        f"[white]{help_text}[/white]",
        title=f"[bold cyan]Help: {command_name}[/bold cyan]",
        border_style="cyan"
    ))

def hvpdb_show_help():
    banner = """
    [bold]High Velocity Python Database[/bold]
    [dim]Next-Gen Data Store for the Modern Web[/dim]
    """
    console.print(Panel(
        banner.strip(),
        title="[bold cyan]HVPDB CLI[/bold cyan]", 
        subtitle="[dim]Enterprise Edition[/dim]",
        border_style="cyan"
    ))

    table = Table(show_header=True, header_style="bold magenta", box=None)
    table.add_column("Category", style="dim", width=15)
    table.add_column("Command", style="green", width=20)
    table.add_column("Description", style="white")

    # Core Commands
    table.add_row("Core", "init", "Initialize database")
    table.add_row("", "shell", "Start interactive shell")
    table.add_row("", "deploy", "Start API Server")
    table.add_row("", "help", "Show this help or command help")
    
    # Data Management
    table.add_row("Data", "import", "Import JSON file")
    table.add_row("", "export", "Export database to JSON")
    table.add_row("", "dump", "Dump search results")
    table.add_row("", "diff", "Compare documents")
    
    # Structure
    table.add_row("Structure", "create-group", "Create a new group")
    table.add_row("", "drop-group", "Delete a group")
    table.add_row("", "jump", "Open shell in group")

    # Access Control
    table.add_row("Access Control", "users", "List users")
    table.add_row("", "create-user", "Create new user")
    table.add_row("", "grant", "Grant permissions")
    table.add_row("", "revoke", "Revoke permissions")

    # Maintenance
    table.add_row("Maintenance", "passwd", "Change password")
    table.add_row("", "backup", "Backup database")
    table.add_row("", "compact", "Compact storage")
    table.add_row("", "stats", "Show statistics")
    table.add_row("", "drop-db", "Delete database")

    console.print(table)
    console.print("\n[dim]Tip: Use 'hvpdb help <command>' for detailed usage.[/dim]")
    
    console.print("\n[bold underline]Usage Examples:[/bold underline]")
    
    # Example 1: Init
    console.print("  [white]hvpdb[/white] [bold cyan]init[/bold cyan] [yellow]my_db[/yellow]")
    
    # Example 2: Deploy
    console.print("  [white]hvpdb[/white] [bold cyan]deploy[/bold cyan] [yellow]my_db[/yellow] [blue]8080[/blue]")

if __name__ == "__main__":
    # Intercept custom help flags
    help_triggers = {
        "-h", "-H", "--h", "--H", 
        "-help", "--help", "--HELP", "-HELP", 
        "help", "HELP"
    }
    
    if len(sys.argv) > 1 and sys.argv[1] in help_triggers:
        sys.argv = [sys.argv[0], "help"]
        
    app()
