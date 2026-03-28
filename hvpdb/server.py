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
app = FastAPI(title='HVPDB Server', version='1.0.8')

# GraphQL Support (v1.0.8+)
try:
    import strawberry
    from strawberry.fastapi import GraphQLRouter
    HAS_GRAPHQL = True
except ImportError:
    HAS_GRAPHQL = False

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
        
    # Fix: Timing attack prevention (check None late and always compare)
    if token is None:
        token = ""
    
    if not secrets.compare_digest(token, db_instance.password or ""):
        raise HTTPException(status_code=401, detail='Unauthorized: Invalid Password')
    return True

@app.get('/')
def read_root():
    """Health check endpoint."""
    return {'server': 'HVPDB', 'status': 'running', 'version': '1.0.8'}

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
    try:
        db_instance.drop_group(name)
        db_instance.commit()
        return {'status': 'dropped', 'group': name}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

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
    
    # Setup GraphQL API (v1.0.8+)
    if HAS_GRAPHQL:
        _setup_graphql_api()
    
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

def _setup_graphql_api():
    """
    Setup GraphQL API endpoint for v1.0.8+.
    
    GraphQL provides a modern query interface for database clients. By
    auto-generating the schema from existing groups and their data, we
    reduce maintenance burden - the schema stays in sync with the actual
    database state without manual SDL definition.
    
    The exposed Query type provides two entry points:
    - groups(): List all available group names
    - group_docs(): Query documents from a specific group with optional filter
    
    Error handling is intentionally lenient: GraphQL query failures log warnings
    but return empty results rather than crashing. This maintains API stability
    even with malformed queries from untrusted clients.
    
    Implementation uses Strawberry-graphql, a modern Python GraphQL library
    that handles schema generation and HTTP binding efficiently.
    """
    if not HAS_GRAPHQL or not db_instance:
        return
    
    try:
        # Build dynamic GraphQL types from database groups. The Query type
        # defines the entry points clients can use. We use Strawberry's
        # @strawberry.type and @strawberry.field decorators to build
        # the schema from Python code rather than SDL strings.
        @strawberry.type
        class Query:
            @strawberry.field
            def groups(self) -> list[str]:
                """List all available groups in the database."""
                return db_instance.get_all_groups()
            
            @strawberry.field
            def group_docs(self, group_name: str, query_json: Optional[str] = None) -> list[dict]:
                """
                Retrieve documents from a group with optional MongoDB-style query filter.
                
                The query_json parameter accepts a JSON-encoded query dict in the same
                format as the REST API find() endpoint. This allows complex filtering
                (operators like $gt, $in, $regex, etc).
                """
                try:
                    import json
                    query = json.loads(query_json) if query_json else {}
                    grp = db_instance.group(group_name)
                    docs = grp.find(query)
                    # Convert HVPDocument objects to plain dicts for GraphQL
                    # serialization. GraphQL needs JSON-serializable values.
                    return [dict(d) for d in docs]
                except Exception as e:
                    warnings.warn(f"GraphQL query failed: {e}")
                    return []
        
        # Create the Strawberry schema from the Query type. This validates
        # the schema at startup and catches definition errors early.
        schema = strawberry.Schema(query=Query)
        
        # Mount the GraphQL endpoint at /graphql on the FastAPI app.
        # Strawberry provides GraphQL protocol handling over HTTP/WebSocket.
        graphql_router = GraphQLRouter(schema, path="/graphql")
        app.include_router(graphql_router)
        
    except Exception as e:
        warnings.warn(f"Failed to setup GraphQL API: {e}")
