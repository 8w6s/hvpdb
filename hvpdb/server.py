import os
import secrets
import socket
import warnings
from typing import Any, Dict, Optional

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

from .core import HVPDB

db_instance: Optional[HVPDB] = None
app = FastAPI(title='HVPDB Server', version='1.0.0')

class QueryModel(BaseModel):
    """Schema for query requests."""
    query: Dict[str, Any] = {}

class InsertModel(BaseModel):
    """Schema for insertion requests."""
    data: Dict[str, Any]

class UpdateModel(BaseModel):
    """Schema for update requests."""
    query: Dict[str, Any]
    update: Dict[str, Any]

def get_auth(authorization: Optional[str]=Header(None), x_hvp_key: Optional[str]=Header(None)):
    """
    Dependency for validating API requests against the database password.
    """
    if not db_instance or not db_instance.password:
        return True
    
    token = None
    if authorization and authorization.startswith('Bearer '):
        token = authorization.split(' ')[1]
    elif x_hvp_key:
        token = x_hvp_key
        
    if token is None or not secrets.compare_digest(token, db_instance.password):
        raise HTTPException(status_code=401, detail='Unauthorized: Invalid Password')
    return True

@app.get('/')
def read_root():
    """Health check endpoint."""
    return {'server': 'HVPDB', 'status': 'running', 'version': '1.0.3.dev1'}

@app.get('/groups', dependencies=[Depends(get_auth)])
def list_groups():
    """List all available groups in the database."""
    if db_instance is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    return {'groups': db_instance.get_all_groups()}

@app.post('/group/{name}/find', dependencies=[Depends(get_auth)])
def find_docs(name: str, q: QueryModel):
    """Find documents in a specific group matching the query."""
    if db_instance is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    grp = db_instance.group(name)
    return grp.find(q.query)

@app.post('/group/{name}/insert', dependencies=[Depends(get_auth)])
def insert_doc(name: str, item: InsertModel):
    """Insert a new document into a group."""
    if db_instance is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    grp = db_instance.group(name)
    res = grp.insert(item.data)
    db_instance.commit()
    return res

@app.post('/group/{name}/update', dependencies=[Depends(get_auth)])
def update_doc(name: str, item: UpdateModel):
    """Update documents in a group matching the query."""
    if db_instance is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    grp = db_instance.group(name)
    count = grp.update(item.query, item.update)
    db_instance.commit()
    return {'updated': count}

@app.delete('/group/{name}/delete', dependencies=[Depends(get_auth)])
def delete_doc(name: str, q: QueryModel):
    """Delete documents from a group matching the query."""
    if db_instance is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    grp = db_instance.group(name)
    count = grp.delete(q.query)
    db_instance.commit()
    return {'deleted': count}

@app.delete('/group/{name}/drop', dependencies=[Depends(get_auth)])
def drop_group(name: str):
    """Drop an entire group from the database."""
    if db_instance is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    if name in db_instance.storage.data['groups']:
        del db_instance.storage.data['groups'][name]
        db_instance.storage._dirty = True
        db_instance.commit()
        return {'status': 'dropped', 'group': name}
    raise HTTPException(status_code=404, detail='Group not found')

def start_server(db_path: str, password: Optional[str]=None, host: str='0.0.0.0', port: int=2321):
    """
    Launch the HVPDB FastAPI server.
    
    Args:
        db_path: Path to the database file.
        password: Optional master password for the database.
        host: Host address to bind the server.
        port: Port to listen on.
    """
    global db_instance
    if not password:
        password = os.environ.get('HVPDB_PASSWORD')
    if not password:
        raise ValueError('Auth Error: Password required. Set HVPDB_PASSWORD or pass password explicitly.')

    db_instance = HVPDB(db_path, password)
    
    hostname = socket.gethostname()
    try:
        local_ip = socket.gethostbyname(hostname)
    except Exception as e:
        warnings.warn(f"Failed to resolve hostname '{hostname}', falling back to 127.0.0.1: {e}")
        local_ip = '127.0.0.1'
        
    uri = f'hvp://{local_ip}:{port}'
    print(f'\n🚀 HVPDB Server deployed at: {uri}')
    print(f'📂 Database: {db_path}')
    
    print('🔒 Auth: Enabled')
        
    print('\nPress Ctrl+C to stop.\n')
    uvicorn.run(app, host=host, port=port, log_level='info')
