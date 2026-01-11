import os
import struct
import zlib
import msgpack
import warnings
import datetime
from typing import Dict, Any, List, Optional
import zstandard as zstd
from .wal import HVPWAL, WAL_MAGIC, WAL_VERSION, MAX_ENTRY_SIZE
from .core import HVPDB
from .utils import normalize_target

class Diagnostics:

    def __init__(self, target: str, password: str=None):
        self.target = normalize_target(target)
        self.password = password
        self.wal_path = self.target + '.log'

    def doctor(self) -> Dict[str, Any]:
        report = {'target': self.target, 'exists': os.path.exists(self.target), 'wal_exists': os.path.exists(self.wal_path), 'status': 'healthy', 'issues': []}
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
                if st.st_mode & 63:
                    report['issues'].append('Insecure file permissions (should be 0600).')
            except:
                pass
        return report

    def wal_status(self) -> Dict[str, Any]:
        if not os.path.exists(self.wal_path):
            return {'status': 'missing'}
        stats = {'size': os.path.getsize(self.wal_path), 'entries': 0, 'last_seq': 0, 'pending_txns': 0, 'corrupt': False}
        try:
            with open(self.wal_path, 'rb') as f:
                magic = f.read(6)
                if magic == WAL_MAGIC:
                    version = int.from_bytes(f.read(2), 'big')
                    f.read(16)
                    kdf_len = int.from_bytes(f.read(2), 'big')
                    f.read(kdf_len)
                else:
                    f.seek(0)
                while True:
                    header = f.read(8)
                    if not header or len(header) < 8:
                        break
                    _, length = struct.unpack('>II', header)
                    if length == 0 or length > MAX_ENTRY_SIZE:
                        stats['corrupt'] = True
                        break
                    f.seek(12 + length, 1)
                    stats['entries'] += 1
        except Exception:
            stats['corrupt'] = True
        return stats

    def wal_dump(self, limit: int=200) -> List[Dict]:
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
        results = self.doctor()
        if results['status'] != 'healthy' and results['status'] != 'healthy_with_warnings':
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
        if not self.password:
            raise ValueError('Password required for checkpoint.')
        db = HVPDB(self.target, self.password)
        db.commit()