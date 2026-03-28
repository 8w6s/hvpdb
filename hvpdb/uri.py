import urllib.parse
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class HVPConnectionInfo:
    """
    Data class representing a parsed HVP connection URI.
    """
    scheme: str
    username: Optional[str]
    password: Optional[str]
    cluster: Optional[str]
    shards: List[str]
    database: str
    options: Dict[str, str]

    @property
    def connection_string(self) -> str:
        """
        Construct a redacted connection string for display/logging.
        """
        auth = f'{self.username}:****@' if self.username else ''
        hosts = self.cluster if self.cluster else ''
        if self.shards:
            hosts += '~' + ','.join(self.shards)
        query = '&'.join([f'{k}={v}' for k, v in self.options.items()])
        query_part = f'?{query}' if query else ''
        return f'{self.scheme}://{auth}{hosts}/{self.database}{query_part}'

class HVPURI:
    """
    Utility class for parsing HVP connection URIs.
    """

    @staticmethod
    def parse(uri: str) -> HVPConnectionInfo:
        """
        Parse an HVP URI into an HVPConnectionInfo object.
        
        Format: hvp://[user:pass@]cluster[~shard1,shard2]/db[?opt1=val1&opt2=val2]
        
        Args:
            uri: The HVP URI string.
            
        Returns:
            HVPConnectionInfo object.
            
        Raises:
            ValueError: If the URI scheme is invalid.
        """
        if not (uri.startswith('hvp://') or uri.startswith('hvpdb://')):
            raise ValueError('Invalid Scheme. Must start with hvp:// or hvpdb://')
        
        prefix_len = 6 if uri.startswith('hvp://') else 8
        rest = uri[prefix_len:]
        username = None
        password = None
        
        if '@' in rest:
            # Fix: Use rsplit to correctly handle '@' in passwords (standard URI parsing)
            # The authority section is terminated by the last '@' before the host
            auth_part, rest = rest.rsplit('@', 1)
            if ':' in auth_part:
                username, password = auth_part.split(':', 1)
            else:
                password = auth_part
            username = urllib.parse.unquote(username) if username else None
            password = urllib.parse.unquote(password) if password else None
            
        if '/' in rest:
            host_part, path_query = rest.split('/', 1)
        elif '?' in rest:
            host_part, path_query = rest.split('?', 1)
            path_query = '?' + path_query # Re-add ? for the path_query logic below
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
        if path_query.startswith('?'):
             database = 'default'
             query_part = path_query[1:]
             for pair in query_part.split('&'):
                if '=' in pair:
                    k, v = pair.split('=', 1)
                    options[urllib.parse.unquote(k)] = urllib.parse.unquote(v)
        elif '?' in path_query:
            path_part, query_part = path_query.split('?', 1)
            database = urllib.parse.unquote(path_part)
            for pair in query_part.split('&'):
                if '=' in pair:
                    k, v = pair.split('=', 1)
                    options[urllib.parse.unquote(k)] = urllib.parse.unquote(v)
        else:
            database = urllib.parse.unquote(path_query)
            
        if not database:
            database = 'default'
            
        return HVPConnectionInfo(
            scheme='hvp', 
            username=username, 
            password=password, 
            cluster=cluster, 
            shards=shards, 
            database=database, 
            options=options
        )