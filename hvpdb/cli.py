import sys
import os
import stat
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
    print('Error: CLI dependencies not found.')
    print('Please install with: pip install hvpdb[cli] or pip install typer rich')
    sys.exit(1)
from .core import HVPDB
from .uri import HVPURI
from .utils import redact_target, normalize_target
from .diagnostics import Diagnostics
try:
    if sys.version_info < (3, 10):
        from importlib_metadata import entry_points
    else:
        from importlib.metadata import entry_points
except ImportError:
    entry_points = None
app = typer.Typer(help='HVPDB CLI - High Velocity Python Database', no_args_is_help=False, add_completion=False)
console = Console()
PLUGINS = {}

def load_plugins():
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
                    console.print(f'[yellow]Warning: Failed to load plugin {ep.name}: {e}[/yellow]')
        except Exception as e:
            console.print(f'[red]Plugin discovery error: {e}[/red]')
    known_extensions = ['hvpdb_query', 'hvpdb_perms', 'hvpdb_http', 'hvpdb_backup', 'hvpdb_migrate', 'hvpdb_observe', 'hvpdb_admin', 'hvpdb_tools', 'hvpdb_sync']
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    for ext in known_extensions:
        if ext.replace('hvpdb_', '') in PLUGINS:
            continue
        try:
            module = __import__(ext)
        except ImportError:
            plugin_dir_name = ext.replace('_', '-')
            plugin_path = os.path.join(project_root, plugin_dir_name)
            if os.path.isdir(plugin_path) and plugin_path not in sys.path:
                sys.path.insert(0, plugin_path)
                try:
                    module = __import__(ext)
                except ImportError:
                    continue
            else:
                continue
        short_name = ext.replace('hvpdb_', '')
        PLUGINS[short_name] = module
load_plugins()
for name, plugin in PLUGINS.items():
    if isinstance(plugin, typer.Typer):
        app.add_typer(plugin, name=name)
    elif hasattr(plugin, 'app') and isinstance(plugin.app, typer.Typer):
        app.add_typer(plugin.app, name=name)
if 'query' not in PLUGINS:

    @app.command(name='query', help='Polyglot Query Engine (Missing).\n\nRequires: hvpdb-query', context_settings={'allow_extra_args': True, 'ignore_unknown_options': True})
    def hvpdb_query_placeholder(ctx: typer.Context):
        console.print("[bold red]Error: 'hvpdb-query' plugin is missing.[/bold red]")
        console.print('This command requires the Polyglot Query Engine.')
        console.print('\n[yellow]To install, run:[/yellow]')
        console.print('  [green]pip install hvpdb-query[/green]')
        raise typer.Exit(code=1)

@app.callback(invoke_without_command=True)
def hvpdb_main(ctx: typer.Context):
    if ctx.invoked_subcommand is None:
        hvpdb_show_help()

def hvpdb_get_db(uri_or_path: str, password: str=None) -> HVPDB:
    try:
        if '://' in uri_or_path and '@' in uri_or_path:
            from urllib.parse import urlparse
            try:
                p = urlparse(uri_or_path)
                if p.password:
                    console.print(f'[bold red]SECURITY ERROR:[/bold red] Password embedded in URI is insecure.')
                    console.print('[yellow]Please use environment variable HVPDB_PASSWORD or interactive prompt.[/yellow]')
                    raise typer.Exit(code=1)
            except Exception:
                pass
        if not password:
            password = os.environ.get('HVPDB_PASSWORD')
        if not uri_or_path.startswith('hvp://') and (not password):
            pass
        return HVPDB(uri_or_path, password)
    except Exception as e:
        if 'BadDecrypt' in str(e) or 'password' in str(e).lower():
            console.print('[yellow]Authentication failed or password missing.[/yellow]')
            password = typer.prompt('Enter Database Password', hide_input=True)
            return HVPDB(uri_or_path, password)
        safe_msg = str(e).replace(uri_or_path, redact_target(uri_or_path))
        console.print(f'[bold red]Connection Error:[/bold red] {safe_msg}')
        raise typer.Exit(code=1)

@app.command(name='init', help='Initialize a new database.\n\nUsage: hvpdb init <target> [password]')
def hvpdb_init(target: str=typer.Argument(..., help='File path or URI'), password: Optional[str]=typer.Argument(None, help='Password (Optional - Recommended to omit and use prompt)')):
    if not target.startswith('hvp://') and (not target.endswith('.hvp')) and (not target.endswith('.hvdb')):
        target += '.hvp'
    if os.path.exists(target) and (not target.startswith('hvp://')):
        console.print(f'[yellow]File {target} already exists![/yellow]')
        if not typer.confirm('Do you want to overwrite it?'):
            raise typer.Exit()
    if not target.startswith('hvp://') and (not password):
        password = typer.prompt('New Password', hide_input=True, confirmation_prompt=True)
    elif password:
        console.print('[yellow]Warning: Passing password as argument is insecure. Consider using prompt.[/yellow]')
    try:
        db = HVPDB(target, password)
        db.storage.save()
        abs_path = os.path.abspath(db.filepath)
        console.print(Panel(f'[bold green]Database created successfully![/bold green]\n[bold white]Location:[/bold white] {abs_path}\n[dim]Note: This file is stored in your current project directory.[/dim]', title='Success'))
    except Exception as e:
        console.print(f'[bold red]Init Failed:[/bold red] {e}')

@app.command(name='compact', help='Compact storage.\n\nUsage: hvpdb compact <target> [password]')
def hvpdb_compact(target: str=typer.Argument(..., help='Database Path'), password: Optional[str]=typer.Argument(None, help='Password')):
    db = hvpdb_get_db(target, password)
    console.print('[yellow]Compacting database...[/yellow]')
    db.storage._dirty = True
    if hasattr(db, 'is_cluster') and db.is_cluster:
        for name in db.get_all_groups():
            grp = db.group(name)
            grp.storage._dirty = True
    db.commit()
    console.print('[bold green]Compaction complete![/bold green]')

@app.command(name='snapshot')
def hvpdb_snapshot(target: str=typer.Argument(..., help='Database Target'), output: str=typer.Option(..., '--out', '-o', help='Output file path'), password: Optional[str]=typer.Argument(None, help='Password')):
    db = hvpdb_get_db(target, password)
    db.filepath = output
    db.storage.filepath = output
    db.storage._dirty = True
    db.storage.save()
    console.print(f'[green]Snapshot saved to {output}[/green]')

@app.command(name='pack')
def hvpdb_pack(target: str=typer.Argument(..., help='Database Target'), output: str=typer.Option(..., '--out', '-o', help='Output archive (.hvpz)'), password: Optional[str]=typer.Argument(None, help='Password')):
    import zipfile
    import datetime
    target = normalize_target(target)
    if not output.endswith('.hvpz'):
        output += '.hvpz'
    try:
        with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as zf:
            if os.path.exists(target):
                zf.write(target, arcname='database.hvp')
            else:
                console.print(f'[red]Target {target} not found.[/red]')
                return
            wal_path = target + '.log'
            if os.path.exists(wal_path):
                zf.write(wal_path, arcname='database.hvp.log')
            manifest = {'created_at': str(datetime.datetime.now()), 'target': target, 'version': '1.0'}
            zf.writestr('manifest.json', json.dumps(manifest, indent=2))
        console.print(f'[green]Packed to {output}[/green]')
    except Exception as e:
        console.print(f'[red]Pack failed: {e}[/red]')

@app.command(name='doctor')
def hvpdb_doctor(target: str=typer.Argument(..., help='Database Target')):
    diag = Diagnostics(target)
    report = diag.doctor()
    console.print(f"[bold]Target:[/bold] {report['target']}")
    if report['status'] == 'healthy':
        console.print(f'[green]✓ Status: Healthy[/green]')
    elif report['status'] == 'missing':
        console.print(f'[red]✗ Status: Missing[/red]')
    else:
        console.print(f"[red]✗ Status: {report['status']}[/red]")
    if 'wal_header' in report:
        console.print(f"WAL Header: {report['wal_header']}")
    if report['issues']:
        console.print('[red]Issues found:[/red]')
        for issue in report['issues']:
            console.print(f'  - {issue}')

@app.command(name='verify')
def hvpdb_verify(target: str=typer.Argument(..., help='Database Target'), password: Optional[str]=typer.Argument(None, help='Password'), deep: bool=typer.Option(False, '--deep', help='Deep verification')):
    if not password:
        password = get_db_password()
    diag = Diagnostics(target, password)
    report = diag.verify(deep=deep)
    console.print_json(data=report)
wal_app = typer.Typer(help='WAL Management')
app.add_typer(wal_app, name='wal')

@wal_app.command(name='status')
def wal_status(target: str=typer.Argument(..., help='Database Target')):
    diag = Diagnostics(target)
    stats = diag.wal_status()
    console.print_json(data=stats)

@wal_app.command(name='dump')
def wal_dump(target: str=typer.Argument(..., help='Database Target'), password: Optional[str]=typer.Argument(None, help='Password'), limit: int=typer.Option(200, help='Limit entries')):
    if not password:
        password = get_db_password()
    diag = Diagnostics(target, password)
    try:
        entries = diag.wal_dump(limit)
        console.print_json(data=entries)
    except Exception as e:
        console.print(f'[red]Dump failed: {e}[/red]')

@wal_app.command(name='checkpoint')
def wal_checkpoint(target: str=typer.Argument(..., help='Database Target'), password: Optional[str]=typer.Argument(None, help='Password')):
    if not password:
        password = get_db_password()
    diag = Diagnostics(target, password)
    try:
        diag.checkpoint()
        console.print('[green]Checkpoint successful. WAL truncated.[/green]')
    except Exception as e:
        console.print(f'[red]Checkpoint failed: {e}[/red]')
plugin_app = typer.Typer(help='Plugin Management')
app.add_typer(plugin_app, name='plugin')

@plugin_app.command(name='list')
def plugin_list():
    table = Table(title='Installed Plugins')
    table.add_column('Name', style='cyan')
    table.add_column('Module', style='green')
    for name, plugin in PLUGINS.items():
        module_name = getattr(plugin, '__name__', str(plugin))
        table.add_row(name, module_name)
    console.print(table)

@plugin_app.command(name='info')
def plugin_info(name: str):
    if name not in PLUGINS:
        console.print(f'[red]Plugin {name} not found.[/red]')
        return
    plugin = PLUGINS[name]
    console.print(f'[bold]Plugin:[/bold] {name}')
    console.print(f"Module: {getattr(plugin, '__name__', str(plugin))}")
    if hasattr(plugin, '__doc__'):
        console.print(Panel(plugin.__doc__ or 'No description'))

@plugin_app.command(name='doctor')
def plugin_doctor(name: str):
    if name not in PLUGINS:
        console.print(f'[red]Plugin {name} not found.[/red]')
        return
    console.print(f'[bold]Diagnosing Plugin:[/bold] {name}')
    plugin = PLUGINS[name]
    console.print('[green]✓ Import Successful[/green]')
    if hasattr(plugin, '__version__'):
        console.print(f'[green]✓ Version: {plugin.__version__}[/green]')
    else:
        console.print('[yellow]! Version info missing[/yellow]')
    if hasattr(plugin, 'check_dependencies'):
        try:
            plugin.check_dependencies()
            console.print('[green]✓ Dependencies OK[/green]')
        except Exception as e:
            console.print(f'[red]✗ Dependency Check Failed: {e}[/red]')
    else:
        console.print('[dim]- No dependency check provided[/dim]')

@app.command(name='env')
def hvpdb_env():
    table = Table(title='Environment Variables')
    table.add_column('Variable', style='cyan')
    table.add_column('Value', style='green')
    table.add_column('Description', style='white')
    env_vars = {'HVPDB_PASSWORD': ('******' if os.environ.get('HVPDB_PASSWORD') else 'Not Set', 'Default Database Password'), 'HVPDB_DEBUG': (os.environ.get('HVPDB_DEBUG', 'False'), 'Enable Debug Logging')}
    for key, (val, desc) in env_vars.items():
        table.add_row(key, val, desc)
    console.print(table)
    if not os.environ.get('HVPDB_PASSWORD'):
        console.print('[yellow]Warning: HVPDB_PASSWORD is not set. You will be prompted for passwords.[/yellow]')

@app.command(name='redacted-uri')
def hvpdb_redacted_uri(target: str=typer.Argument(..., help='URI to redact')):
    console.print(redact_target(target))

@app.command(name='create-group', help='Create a new group.\n\nUsage: hvpdb create-group <target> <name> [password]')
def hvpdb_create_group(target: str=typer.Argument(..., help='Database Path'), name: str=typer.Argument(..., help='Group Name'), password: Optional[str]=typer.Argument(None, help='Password')):
    db = hvpdb_get_db(target, password)
    if name in db.get_all_groups():
        console.print(f"[yellow]Group '{name}' already exists.[/yellow]")
        return
    db.group(name)
    db.commit()
    console.print(f"[bold green]Group '{name}' created successfully.[/bold green]")

@app.command(name='drop-group')
def hvpdb_drop_group(target: str=typer.Argument(..., help='Database Path'), name: str=typer.Argument(..., help='Group Name'), password: Optional[str]=typer.Argument(None, help='Password')):
    db = hvpdb_get_db(target, password)
    if name not in db.get_all_groups():
        console.print(f"[red]Group '{name}' not found.[/red]")
        return
    if not typer.confirm(f"Are you sure you want to delete group '{name}'?"):
        return
    if hasattr(db, 'is_cluster') and db.is_cluster:
        group_path = os.path.join(db.filepath, f'{name}.hvp')
        if os.path.exists(group_path):
            os.remove(group_path)
    elif name in db.storage.data['groups']:
        del db.storage.data['groups'][name]
        db.storage._dirty = True
        db.commit()
    console.print(f"[bold green]Group '{name}' deleted.[/bold green]")

@app.command(name='drop-db', help='Destroy the database.\n\nUsage: hvpdb drop-db <target>')
def hvpdb_drop_db(target: str=typer.Argument(..., help='Database Path')):
    if not typer.confirm(f"🔥 DANGER: Are you sure you want to DESTROY database '{target}'?"):
        return
    import shutil
    try:
        if os.path.isdir(target):
            shutil.rmtree(target)
        else:
            if os.path.exists(target):
                os.remove(target)
            wal_path = target + '.wal'
            if os.path.exists(wal_path):
                os.remove(wal_path)
        console.print(f"[bold red]Database '{target}' destroyed.[/bold red]")
    except Exception as e:
        console.print(f'[red]Error:[/red] {e}')

@app.command(name='restore', help='Restore database from backup.\n\nUsage: hvpdb restore <backup_file> --to <target_path>')
def hvpdb_restore(backup_file: str=typer.Argument(..., help='Source Backup File'), to: str=typer.Option(..., '--to', help='Target Database Path'), force: bool=typer.Option(False, help='Overwrite existing database')):
    if not os.path.exists(backup_file):
        console.print(f"[red]Backup file '{backup_file}' not found.[/red]")
        raise typer.Exit(1)
    if os.path.exists(to) and (not force):
        console.print(f"[yellow]Target '{to}' already exists. Use --force to overwrite.[/yellow]")
        raise typer.Exit(1)
    import shutil
    try:
        shutil.copy2(backup_file, to)
        console.print(f"[bold green]Restored database to '{to}' successfully.[/bold green]")
    except Exception as e:
        console.print(f'[red]Restore failed: {e}[/red]')

@app.command(name='repair', help='Attempt to repair a corrupted database.\n\nUsage: hvpdb repair <target>')
def hvpdb_repair(target: str=typer.Argument(..., help='Database Path'), force: bool=typer.Option(False, '--force', help='Force repair even if risky')):
    if not os.path.exists(target):
        console.print(f"[red]Database '{target}' not found.[/red]")
        raise typer.Exit(1)
    if not force and (not typer.confirm('Repair can be destructive. Continue?')):
        return
    console.print('[yellow]Attempting repair...[/yellow]')
    wal_path = target + '.wal'
    if os.path.exists(wal_path):
        try:
            bak_wal = wal_path + '.bak'
            import shutil
            shutil.move(wal_path, bak_wal)
            console.print(f'[green]Moved potentially corrupt WAL to {bak_wal}[/green]')
        except Exception as e:
            console.print(f'[red]Failed to move WAL: {e}[/red]')
    console.print('[green]Repair attempt complete. Try opening the database now.[/green]')

@app.command(name='meta', help='Manage database metadata.\n\nUsage: hvpdb meta <target> [key] [value]')
def hvpdb_meta(target: str=typer.Argument(..., help='Database Path'), key: Optional[str]=typer.Argument(None, help='Metadata Key'), value: Optional[str]=typer.Argument(None, help='Metadata Value (Leave empty to show/unset)'), password: Optional[str]=typer.Argument(None, help='Password'), unset: bool=typer.Option(False, '--unset', help='Remove the key')):
    db = hvpdb_get_db(target, password)
    if 'meta' not in db.storage.data:
        db.storage.data['meta'] = {}
    if not key:
        console.print(Panel(JSON.from_data(db.storage.data['meta']), title='Database Metadata'))
        return
    if unset:
        if key in db.storage.data['meta']:
            del db.storage.data['meta'][key]
            db.storage._dirty = True
            db.commit()
            console.print(f"[green]Metadata '{key}' removed.[/green]")
        else:
            console.print(f"[yellow]Metadata '{key}' not found.[/yellow]")
        return
    if value:
        db.storage.data['meta'][key] = value
        db.storage._dirty = True
        db.commit()
        console.print(f'[green]Metadata set: {key} = {value}[/green]')
    else:
        val = db.storage.data['meta'].get(key, 'Not Set')
        console.print(f'{key}: {val}')

@app.command(name='lock-status')
def hvpdb_lock_status(target: str=typer.Argument(..., help='Database Path')):
    files = [target, target + '.wal']
    locked = []
    for f in files:
        if not os.path.exists(f):
            continue
        try:
            fd = os.open(f, os.O_RDWR | os.O_EXCL)
            os.close(fd)
        except OSError:
            locked.append(f)
    if locked:
        console.print(f'[red]Files locked:[/red]')
        for l in locked:
            console.print(f' - {l}')
        console.print('[yellow]Another process (or this shell) is using them.[/yellow]')
    else:
        console.print('[green]No locks detected (Files are free).[/green]')

@app.command(name='import')
def hvpdb_import(target: str=typer.Argument(..., help='Database Path'), file: str=typer.Argument(..., help='Input file (JSON)'), group: str=typer.Argument('default', help='Target Group'), password: Optional[str]=typer.Argument(None, help='Password')):
    db = hvpdb_get_db(target, password)
    if not os.path.exists(file):
        console.print(f"[red]File '{file}' not found.[/red]")
        return
    with open(file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if isinstance(data, list):
        count = 0
        with console.status('Importing...'):
            for item in data:
                if isinstance(item, dict):
                    db.group(group).insert(item)
                    count += 1
        db.commit()
        console.print(f"[bold green]Imported {count} documents into group '{group}'.[/bold green]")
    elif isinstance(data, dict):
        pass
    else:
        console.print('[red]Invalid JSON format. Expected list of objects.[/red]')

@app.command(name='insert', help='Insert a document.\n\nUsage: hvpdb insert <target> <group> <data> [password]')
def hvpdb_insert(target: str=typer.Argument(..., help='File path or URI'), group: str=typer.Argument(..., help='Group name'), data: str=typer.Argument(..., help='JSON data string'), password: Optional[str]=typer.Argument(None, help='Password')):
    db = hvpdb_get_db(target, password)
    try:
        try:
            doc = json.loads(data)
        except json.JSONDecodeError:
            console.print('[red]Invalid JSON format.[/red]')
            raise typer.Exit(1)
        if not isinstance(doc, dict):
            raise ValueError('Data must be a dictionary')
        res = db.group(group).insert(doc)
        db.commit()
        console.print(f'[bold green]✅ Inserted:[/bold green]')
        console.print(JSON.from_data(res))
    except Exception as e:
        console.print(f'[bold red]❌ Invalid Data:[/bold red] {e}')

@app.command(name='find', help='Find documents.\n\nUsage: hvpdb find <target> <group> [query] [limit] [password]')
def hvpdb_find(target: str=typer.Argument(..., help='File path or URI'), group: str=typer.Argument(..., help='Group name'), query: str=typer.Argument('{}', help='JSON query string'), limit: int=typer.Argument(10, help='Limit results'), password: Optional[str]=typer.Argument(None, help='Password')):
    db = hvpdb_get_db(target, password)
    try:
        try:
            q = json.loads(query)
        except json.JSONDecodeError:
            console.print('[yellow]Invalid JSON query. Using empty query.[/yellow]')
            q = {}
        docs = db.group(group).find(q)
        console.print(f'[bold cyan]🔍 Found {len(docs)} documents (Showing top {limit}):[/bold cyan]')
        for doc in docs[:limit]:
            console.print(JSON.from_data(doc))
            console.print('---')
    except Exception as e:
        console.print(f'[bold red]❌ Error:[/bold red] {e}')

@app.command(name='delete', help='Delete a document by ID.\n\nUsage: hvpdb delete <target> <group> <id> [password]')
def hvpdb_delete(target: str=typer.Argument(..., help='File path or URI'), group: str=typer.Argument(..., help='Group name'), id: str=typer.Argument(..., help='Document ID to delete'), password: Optional[str]=typer.Argument(None, help='Password')):
    db = hvpdb_get_db(target, password)
    count = db.group(group).delete({'_id': id})
    db.commit()
    if count > 0:
        console.print(f'[bold green]Deleted document {id}[/bold green]')
    else:
        console.print(f'[bold yellow]Document {id} not found[/bold yellow]')

@app.command(name='passwd', help='Change password.\n\nUsage: hvpdb passwd <target> [password]')
def hvpdb_passwd(target: str=typer.Argument(..., help='File path or URI'), password: Optional[str]=typer.Argument(None, help='Current Password')):
    db = hvpdb_get_db(target, password)
    new_pass = typer.prompt('Enter New Password', hide_input=True, confirmation_prompt=True)
    if not new_pass:
        console.print('[red]Password cannot be empty![/red]')
        raise typer.Exit(1)
    db.storage.password = new_pass
    db.storage._dirty = True
    db.storage.security = None
    console.print('[yellow]Re-encrypting database...[/yellow]')
    db.commit()
    console.print('[bold green]Password changed successfully![/bold green]')

@app.command(name='shell', help='Start HVPDB Ops Shell (HVPShell).\n\nUsage: hvpdb shell [target] [commands]')
def hvpdb_shell(target: Optional[str]=typer.Argument(None, help='File path or URI'), commands: Optional[str]=typer.Argument(None, help='One-liner commands (sep by +)'), password: Optional[str]=typer.Option(None, help='DEPRECATED: Use passfile/env', hidden=True), passfile: Optional[str]=typer.Option(None, help='Path to file containing password')):
    if password:
        console.print('[bold red]SECURITY ERROR:[/bold red] Password argument/option is forbidden.')
        console.print('[yellow]Use --passfile or HVPDB_PASSWORD env var.[/yellow]')
        raise typer.Exit(code=1)
    from .hvpshell import HVPShell
    if passfile:
        if not os.path.exists(passfile):
            console.print(f"[red]Passfile '{passfile}' not found.[/red]")
            raise typer.Exit(1)
        if os.name == 'posix':
            st = os.stat(passfile)
            if st.st_mode & 63:
                console.print(f"[red]Security Error: Passfile '{passfile}' is too open (must be 0600).[/red]")
                raise typer.Exit(1)
        with open(passfile, 'r') as f:
            password = f.read().strip()
    shell = HVPShell()
    if target:
        try:
            if not password:
                password = os.environ.get('HVPDB_PASSWORD')
            if password:
                shell.db = HVPDB(target, password)
                console.print(f'[green]Connected to {target} (Secure Injection)[/green]')
                shell._update_prompt()
            else:
                shell.onecmd(f'connect {target}')
        except Exception as e:
            console.print(f'[red]Auto-connect failed: {e}[/red]')
    if commands:
        cmds = commands.split('+')
        for cmd in cmds:
            cmd = cmd.strip()
            if cmd:
                shell.onecmd(cmd)
        return
    try:
        shell.cmdloop()
    except KeyboardInterrupt:
        console.print('\n[dim]Session terminated. Bye![/dim]')

@app.command(name='backup')
def hvpdb_backup(target: str=typer.Argument(..., help='Database Path'), output: str=typer.Argument('backup.hvp', help='Backup file path'), password: Optional[str]=typer.Argument(None, help='Password')):
    if os.path.isdir(target):
        console.print('[yellow]Cluster backup not yet supported (copy the folder manually).[/yellow]')
        return
    import shutil
    try:
        shutil.copy2(target, output)
        console.print(f'[bold green]Backup created at {output}[/bold green]')
    except Exception as e:
        console.print(f'[bold red]Backup failed:[/bold red] {e}')

@app.command(name='stats')
def hvpdb_stats(target: str=typer.Argument(..., help='Database Path'), password: Optional[str]=typer.Argument(None, help='Password')):
    db = hvpdb_get_db(target, password)
    size_mb = os.path.getsize(db.filepath) / (1024 * 1024) if hasattr(db, 'filepath') and os.path.exists(db.filepath) else 0
    console.print(f'Size: {size_mb:.2f} MB')
    groups = db.get_all_groups()
    console.print(f'Groups: {len(groups)}')
    for g in groups:
        console.print(f' - {g}: {db.group(g).count()} docs')

def hvpdb_check_perms_pkg():
    if 'perms' not in PLUGINS:
        console.print("[red]Error: 'hvpdb-perms' plugin is not installed.[/red]")
        console.print("[yellow]This command requires the User Management plugin.[/yellow]")
        console.print("\nTo install, run:")
        console.print("  [green]pip install hvpdb-perms[/green]")
        raise typer.Exit(1)

@app.command(name='create-user', help='Create a new user.\n\nUsage: hvpdb create-user <target> <username> [password] [user_password] [role]')
def hvpdb_create_user(target: str=typer.Argument(..., help='Database Path'), username: str=typer.Argument(..., help='New Username'), password: Optional[str]=typer.Argument(None, help='DB Password'), user_password: Optional[str]=typer.Argument(None, help='Password for new user'), role: str=typer.Argument('user', help='Role (user/admin)')):
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
        console.print(f'[red]Error:[/red] {e}')

@app.command(name='grant', help='Grant permission to user.\n\nUsage: hvpdb grant <target> <username> <group> [password]')
def hvpdb_grant(target: str=typer.Argument(..., help='Database Path'), username: str=typer.Argument(..., help='Username'), group: str=typer.Argument(..., help='Group to grant access to'), password: Optional[str]=typer.Argument(None, help='DB Password')):
    hvpdb_check_perms_pkg()
    db = hvpdb_get_db(target, password)
    pm = PLUGINS['perms'](db)
    try:
        pm.grant(username, group)
        db.commit()
        console.print(f"[bold green]Granted access to '{group}' for user '{username}'.[/bold green]")
    except Exception as e:
        console.print(f'[red]Error:[/red] {e}')

@app.command(name='revoke', help='Revoke permission from user.\n\nUsage: hvpdb revoke <target> <username> <group> [password]')
def hvpdb_revoke(target: str=typer.Argument(..., help='Database Path'), username: str=typer.Argument(..., help='Username'), group: str=typer.Argument(..., help='Group to revoke access from'), password: Optional[str]=typer.Argument(None, help='DB Password')):
    hvpdb_check_perms_pkg()
    db = hvpdb_get_db(target, password)
    pm = PLUGINS['perms'](db)
    try:
        pm.revoke(username, group)
        db.commit()
        console.print(f"[bold green]Revoked access to '{group}' from user '{username}'.[/bold green]")
    except Exception as e:
        console.print(f'[red]Error:[/red] {e}')

@app.command(name='users', help='List all users.\n\nUsage: hvpdb users <target> [password]')
def hvpdb_list_users(target: str=typer.Argument(..., help='Database Path'), password: Optional[str]=typer.Argument(None, help='DB Password')):
    hvpdb_check_perms_pkg()
    db = hvpdb_get_db(target, password)
    pm = PLUGINS['perms'](db)
    users = pm.list_users()
    table = Table(title='Database Users')
    table.add_column('Username', style='cyan')
    table.add_column('Role', style='magenta')
    table.add_column('Groups', style='green')
    for u, data in users.items():
        groups = ', '.join(data.get('groups', []))
        table.add_row(u, data.get('role'), groups)
    console.print(table)

@app.command(name='export')
def hvpdb_export(target: str=typer.Argument(..., help='File path or URI'), output: str=typer.Argument('dump.json', help='Output JSON file'), password: Optional[str]=typer.Argument(None, help='Password')):
    db = hvpdb_get_db(target, password)
    data = db.storage.data
    with open(output, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str)
    console.print(f'[bold green]✅ Exported to {output}[/bold green]')

@app.command(name='deploy', help='Deploy HVPDB as a Network Server.\n\nUsage: hvpdb deploy <target> [port] [host]')
def hvpdb_deploy(target: str=typer.Argument(..., help='Database Path'), port: int=typer.Argument(2321, help='Port to listen on'), host: str=typer.Argument('127.0.0.1', help='Host to bind (Default: localhost)'), password: Optional[str]=typer.Option(None, help='Database Password (Prompt if missing)')):
    from .server import start_server
    if not password:
        password = typer.prompt('Database Password', hide_input=True)
    if not os.path.exists(target):
        console.print(f"[yellow]Database '{target}' does not exist. Initializing...[/yellow]")
        HVPDB(target, password).storage.save()
    try:
        start_server(target, password, host, port)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        console.print(f'[bold red]Server Error:[/bold red] {e}')

@app.command(name='diff', help='Compare two documents.\n\nUsage: hvpdb diff <target> <group> <id1> <id2> [password]')
def hvpdb_diff(target: str=typer.Argument(..., help='Database Path'), group: str=typer.Argument(..., help='Group Name'), id1: str=typer.Argument(..., help='First Document ID'), id2: str=typer.Argument(..., help='Second Document ID'), password: Optional[str]=typer.Argument(None, help='Password')):
    db = hvpdb_get_db(target, password)
    grp = db.group(group)
    doc1 = grp.find_one({'_id': id1})
    doc2 = grp.find_one({'_id': id2})
    if not doc1:
        console.print(f'[red]Document {id1} not found.[/red]')
        return
    if not doc2:
        console.print(f'[red]Document {id2} not found.[/red]')
        return
    json1 = json.dumps(doc1, indent=2, sort_keys=True).splitlines()
    json2 = json.dumps(doc2, indent=2, sort_keys=True).splitlines()
    diff = difflib.unified_diff(json1, json2, fromfile=id1, tofile=id2, lineterm='')
    for line in diff:
        if line.startswith('+'):
            console.print(f'[green]{line}[/green]')
        elif line.startswith('-'):
            console.print(f'[red]{line}[/red]')
        elif line.startswith('^'):
            console.print(f'[blue]{line}[/blue]')
        else:
            console.print(line)

@app.command(name='jump', help='Open shell in specific group.\n\nUsage: hvpdb jump <target> <group> [password]')
def hvpdb_jump(target: str=typer.Argument(..., help='Database Path'), group: str=typer.Argument(..., help='Group to jump into'), password: Optional[str]=typer.Argument(None, help='Password')):
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
        console.print('\n[dim]Session terminated. Bye![/dim]')

@app.command(name='dump', help='Dump search results.\n\nUsage: hvpdb dump <target> <group> [query] [output] [password]')
def hvpdb_dump(target: str=typer.Argument(..., help='Database Path'), group: str=typer.Argument(..., help='Group Name'), query: str=typer.Argument('{}', help='Query JSON'), output: str=typer.Argument('dump.json', help='Output file'), password: Optional[str]=typer.Argument(None, help='Password')):
    db = hvpdb_get_db(target, password)
    try:
        q = json.loads(query)
    except json.JSONDecodeError:
        console.print('[yellow]Invalid JSON query. Using empty query.[/yellow]')
        q = {}
    docs = db.group(group).find(q)
    with open(output, 'w', encoding='utf-8') as f:
        json.dump(docs, f, indent=2, default=str)
    console.print(f'[bold green]Dumped {len(docs)} documents to {output}[/bold green]')

@app.command(name='version')
def hvpdb_version():
    from . import __version__ as pkg_version
    console.print(f'[bold cyan]HVPDB v{pkg_version}[/bold cyan]')
    console.print('Engine: HVP-Storage (Python)')

@app.command(name='help')
def hvpdb_help(command: Optional[str]=typer.Argument(None, help='Command to get help for')):
    if command:
        hvpdb_show_command_help(command)
    else:
        hvpdb_show_help()

def hvpdb_show_command_help(command_name: str):
    cmd_func = None
    for cmd in app.registered_commands:
        if cmd.name == command_name or command_name in cmd.name.split():
            cmd_func = cmd
            break
    if not cmd_func:
        console.print(f"[red]Command '{command_name}' not found.[/red]")
        return
    help_text = cmd_func.help or 'No description available.'
    console.print(Panel(f'[white]{help_text}[/white]', title=f'[bold cyan]Help: {command_name}[/bold cyan]', border_style='cyan'))

def hvpdb_show_help():
    banner = '\n    [bold]High Velocity Python Database[/bold]\n    [dim]Next-Gen Data Store for the Modern Web[/dim]\n    '
    console.print(Panel(banner.strip(), title='[bold cyan]HVPDB CLI[/bold cyan]', subtitle='[dim]Enterprise Edition[/dim]', border_style='cyan'))
    table = Table(show_header=True, header_style='bold magenta', box=None)
    table.add_column('Category', style='dim', width=15)
    table.add_column('Command', style='green', width=20)
    table.add_column('Description', style='white')
    table.add_row('Core', 'init', 'Initialize database')
    table.add_row('', 'shell', 'Start interactive shell')
    table.add_row('', 'deploy', 'Start API Server')
    table.add_row('', 'help', 'Show this help or command help')
    table.add_row('Data', 'import', 'Import JSON file')
    table.add_row('', 'export', 'Export database to JSON')
    table.add_row('', 'dump', 'Dump search results')
    table.add_row('', 'diff', 'Compare documents')
    table.add_row('Structure', 'create-group', 'Create a new group')
    table.add_row('', 'drop-group', 'Delete a group')
    table.add_row('', 'jump', 'Open shell in group')
    table.add_row('Access Control', 'users', 'List users')
    table.add_row('', 'create-user', 'Create new user')
    table.add_row('', 'grant', 'Grant permissions')
    table.add_row('', 'revoke', 'Revoke permissions')
    table.add_row('Maintenance', 'doctor', 'Check database health')
    table.add_row('', 'verify', 'Verify integrity')
    table.add_row('', 'wal', 'WAL Management')
    table.add_row('', 'snapshot', 'Export snapshot')
    table.add_row('', 'pack', 'Pack database archive')
    table.add_row('', 'plugin', 'Plugin Manager')
    table.add_row('', 'passwd', 'Change password')
    table.add_row('', 'backup', 'Backup database')
    table.add_row('', 'compact', 'Compact storage')
    table.add_row('', 'stats', 'Show statistics')
    table.add_row('', 'drop-db', 'Delete database')
    if PLUGINS:
        first = True
        for name, plugin in PLUGINS.items():
            if name == 'perms':
                continue
            desc = 'External Plugin'
            if hasattr(plugin, 'app') and hasattr(plugin.app, 'info') and plugin.app.info.help:
                desc = plugin.app.info.help
            elif hasattr(plugin, '__doc__') and plugin.__doc__:
                desc = plugin.__doc__.strip().split('\n')[0]
            table.add_row('Plugins' if first else '', name, desc)
            first = False
    console.print(table)
    console.print("\n[dim]Tip: Use 'hvpdb help <command>' for detailed usage.[/dim]")
    console.print('\n[bold underline]Usage Examples:[/bold underline]')
    console.print('  [white]hvpdb[/white] [bold cyan]init[/bold cyan] [yellow]my_db[/yellow]')
    console.print('  [white]hvpdb[/white] [bold cyan]deploy[/bold cyan] [yellow]my_db[/yellow] [blue]8080[/blue]')
if __name__ == '__main__':
    help_triggers = {'-h', '-H', '--h', '--H', '-help', '--help', '--HELP', '-HELP', 'help', 'HELP'}
    if len(sys.argv) > 1 and sys.argv[1] in help_triggers:
        sys.argv = [sys.argv[0], 'help']
    app()