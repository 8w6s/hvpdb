import uvicorn
from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import os
import socket
from .core import HVPDB
db_instance: Optional[HVPDB] = None
app = FastAPI(title='HVPDB Server', version='1.0.0')

class QueryModel(BaseModel):
    query: Dict[str, Any] = {}

class InsertModel(BaseModel):
    data: Dict[str, Any]

class UpdateModel(BaseModel):
    query: Dict[str, Any]
    update: Dict[str, Any]

def get_auth(authorization: Optional[str]=Header(None), x_hvp_key: Optional[str]=Header(None)):
    if not db_instance or not db_instance.password:
        return True
    token = None
    if authorization and authorization.startswith('Bearer '):
        token = authorization.split(' ')[1]
    elif x_hvp_key:
        token = x_hvp_key
    if token != db_instance.password:
        raise HTTPException(status_code=401, detail='Unauthorized: Invalid Password')
    return True

@app.get('/')
def read_root():
    return {'server': 'HVPDB', 'status': 'running', 'version': '1.0.2.post1'}

@app.get('/groups', dependencies=[Depends(get_auth)])
def list_groups():
    return {'groups': db_instance.get_all_groups()}

@app.post('/group/{name}/find', dependencies=[Depends(get_auth)])
def find_docs(name: str, q: QueryModel):
    grp = db_instance.group(name)
    return grp.find(q.query)

@app.post('/group/{name}/insert', dependencies=[Depends(get_auth)])
def insert_doc(name: str, item: InsertModel):
    grp = db_instance.group(name)
    res = grp.insert(item.data)
    db_instance.commit()
    return res

@app.post('/group/{name}/update', dependencies=[Depends(get_auth)])
def update_doc(name: str, item: UpdateModel):
    grp = db_instance.group(name)
    count = grp.update(item.query, item.update)
    db_instance.commit()
    return {'updated': count}

@app.delete('/group/{name}/delete', dependencies=[Depends(get_auth)])
def delete_doc(name: str, q: QueryModel):
    grp = db_instance.group(name)
    count = grp.delete(q.query)
    db_instance.commit()
    return {'deleted': count}

@app.delete('/group/{name}/drop', dependencies=[Depends(get_auth)])
def drop_group(name: str):
    if name in db_instance.storage.data['groups']:
        del db_instance.storage.data['groups'][name]
        db_instance.storage._dirty = True
        db_instance.commit()
        return {'status': 'dropped', 'group': name}
    raise HTTPException(status_code=404, detail='Group not found')

def start_server(db_path: str, password: str=None, host: str='0.0.0.0', port: int=2321):
    global db_instance
    db_instance = HVPDB(db_path, password)
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    uri = f'hvp://{local_ip}:{port}'
    print(f'\n🚀 HVPDB Server deployed at: {uri}')
    print(f'📂 Database: {db_path}')
    if password:
        print('🔒 Auth: Enabled')
    else:
        print('⚠️ Auth: Disabled (Public Access)')
    print('\nPress Ctrl+C to stop.\n')
    uvicorn.run(app, host=host, port=port, log_level='info')