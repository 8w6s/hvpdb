import os
import struct
import warnings
from typing import Any, Dict, List, Optional

from .core import HVPDB
from .utils import normalize_target
from .wal import HVPWAL, MAX_ENTRY_SIZE, WAL_MAGIC


class Diagnostics:
    """
    Diagnostic tools for checking HVPDB health and inspecting WAL logs.
    """

    def __init__(self, target: str, password: Optional[str]=None):
        """
        Initialize the diagnostic tool.
        
        Args:
            target: Path to the database file.
            password: Password for encrypted content access.
        """
        self.target = normalize_target(target)
        self.password = password
        self.wal_path = self.target + '.log'

    def doctor(self) -> Dict[str, Any]:
        """
        Perform a health check on the database file and its permissions.
        
        Returns:
            Dictionary containing health status and identified issues.
        """
        report = {
            'target': self.target, 
            'exists': os.path.exists(self.target), 
            'wal_exists': os.path.exists(self.wal_path), 
            'status': 'healthy', 
            'issues': []
        }
        
        if not report['exists']:
            report['status'] = 'missing'
            report['issues'].append('Database file not found.')
            return report
            
        try:
            with open(self.target, 'rb') as f:
                header = f.read(5)
                if header != b'HVPDB':
                    report['issues'].append('Invalid Database Header.')
                    report['status'] = 'corrupt'
        except Exception as e:
            report['issues'].append(f'Cannot read DB file: {e}')
            report['status'] = 'error'
            
        if report['wal_exists']:
            try:
                salt, kdf = HVPWAL.read_header(self.wal_path)
                report['wal_header'] = 'v2' if salt else 'legacy/missing'
            except Exception as e:
                report['issues'].append(f'WAL Read Error: {e}')
                
        if os.name != 'nt':
            try:
                st = os.stat(self.target)
                # Check for group/other read/write/execute permissions (0o077)
                if st.st_mode & 0o077:
                    report['issues'].append('Insecure file permissions (should be 0600).')
                    report['status'] = 'healthy_with_warnings'
            except OSError as e:
                report['issues'].append(f'Cannot check file permissions: {e}')
                if report['status'] == 'healthy':
                    report['status'] = 'healthy_with_warnings'
                
        return report

    def wal_status(self) -> Dict[str, Any]:
        """
        Inspect the WAL file and provide basic statistics.
        
        Returns:
            Dictionary with WAL size, entry count, and corruption status.
        """
        if not os.path.exists(self.wal_path):
            return {'status': 'missing'}
            
        stats = {
            'size': os.path.getsize(self.wal_path), 
            'entries': 0, 
            'last_seq': 0, 
            'pending_txns': 0, 
            'corrupt': False
        }
        
        try:
            with open(self.wal_path, 'rb') as f:
                magic = f.read(6)
                if magic == WAL_MAGIC:
                    f.read(2)
                    f.read(16) # Skip nonce
                    kdf_len = int.from_bytes(f.read(2), 'big')
                    f.read(kdf_len) # Skip KDF params
                else:
                    f.seek(0)
                    
                while True:
                    current_pos = f.tell()
                    header = f.read(8)
                    if not header or len(header) < 8:
                        break
                    
                    seq, length = struct.unpack('>II', header)
                    if length == 0 or length > MAX_ENTRY_SIZE:
                        stats['corrupt'] = True
                        break
                    
                    # Verify if we can actually seek to the end of this entry
                    try:
                        f.seek(length + 4, 1) # Skip data (length) + checksum (4)
                        stats['entries'] += 1
                        stats['last_seq'] = seq
                    except (OSError, ValueError):
                        stats['corrupt'] = True
                        f.seek(current_pos)
                        break
        except Exception as e:
            warnings.warn(f"WAL scan error for {self.wal_path}: {e}")
            stats['corrupt'] = True
        return stats

    def wal_dump(self, limit: int=200) -> List[Dict]:
        """
        Decrypted dump of WAL entries.
        
        Args:
            limit: Maximum number of entries to return.
            
        Returns:
            List of decrypted WAL entries.
            
        Raises:
            ValueError: If password is not provided.
        """
        if not self.password:
            raise ValueError('Password required to dump WAL.')
            
        from .security import HVPSecurity
        salt, kdf_params = HVPWAL.read_header(self.wal_path)
        
        if not salt:
            security = HVPSecurity(self.password)
        else:
            security = HVPSecurity(self.password, salt, kdf_params)
            
        wal = HVPWAL(self.wal_path, security)
        entries = []

        def collector(entry):
            if len(entries) < limit:
                entries.append(entry)
        
        wal.replay(0, collector)
        return entries

    def verify(self, deep: bool=False) -> Dict[str, Any]:
        """
        Verify database and WAL integrity.
        
        Args:
            deep: Whether to perform a deep scan (not yet fully implemented).
            
        Returns:
            Comprehensive health report.
        """
        results = self.doctor()
        if results['status'] not in ('healthy', 'healthy_with_warnings'):
            return results
            
        if not self.password:
            results['issues'].append('Cannot verify content integrity without password.')
            return results
            
        try:
            db = HVPDB(self.target, self.password)
            results['group_count'] = len(db.storage.data.get('groups', {}))
            results['sequence'] = db.storage._last_sequence
            wal_stats = self.wal_status()
            
            if wal_stats['entries'] > 0 and db.storage._last_sequence == 0 and (wal_stats['size'] > 100):
                results['issues'].append('Warning: DB sequence is 0 but WAL has entries. Possible data loss or fresh snapshot.')
        except Exception as e:
            results['status'] = 'corrupt'
            results['issues'].append(f'Integrity Check Failed: {e}')
            
        return results

    def checkpoint(self):
        """
        Manually trigger a database checkpoint (flush WAL to storage).
        
        Raises:
            ValueError: If password is not provided.
        """
        if not self.password:
            raise ValueError('Password required for checkpoint.')
        db = HVPDB(self.target, self.password)
        db.commit()
