import urllib.parse
from typing import List, Dict, Optional
from dataclasses import dataclass, field

@dataclass
class HVPConnectionInfo:
    scheme: str
    username: Optional[str]
    password: Optional[str]
    cluster: Optional[str]
    shards: List[str]
    database: str
    options: Dict[str, str]

    @property
    def connection_string(self) -> str:
        auth = f'{self.username}:****@' if self.username else ''
        hosts = self.cluster if self.cluster else ''
        if self.shards:
            hosts += '~' + ','.join(self.shards)
        query = '&'.join([f'{k}={v}' for k, v in self.options.items()])
        query_part = f'?{query}' if query else ''
        return f'{self.scheme}://{auth}{hosts}/{self.database}{query_part}'

class HVPURI:

    @staticmethod
    def parse(uri: str) -> HVPConnectionInfo:
        if not uri.startswith('hvp://'):
            raise ValueError('Invalid Scheme. Must start with hvp://')
        rest = uri[6:]
        username = None
        password = None
        if '@' in rest:
            auth_part, rest = rest.split('@', 1)
            if ':' in auth_part:
                username, password = auth_part.split(':', 1)
            else:
                password = auth_part
            username = urllib.parse.unquote(username) if username else None
            password = urllib.parse.unquote(password) if password else None
        if '/' in rest:
            host_part, path_query = rest.split('/', 1)
        else:
            host_part = rest
            path_query = ''
        cluster = None
        shards = []
        if '~' in host_part:
            cluster, shards_part = host_part.split('~', 1)
            shards = shards_part.split(',')
        else:
            cluster = host_part
        database = ''
        options = {}
        if '?' in path_query:
            path_part, query_part = path_query.split('?', 1)
            database = path_part
            for pair in query_part.split('&'):
                if '=' in pair:
                    k, v = pair.split('=', 1)
                    options[k] = v
        else:
            database = path_query
        if not database:
            database = 'default'
        return HVPConnectionInfo(scheme='hvp', username=username, password=password, cluster=cluster, shards=shards, database=database, options=options)