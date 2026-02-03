class HVPError(Exception):
    """Base exception class for all HVPDB related errors."""
    pass

class AuthError(HVPError):
    """Raised when authentication fails (invalid password)."""
    pass

class ConsistencyError(HVPError):
    """Raised when a database consistency issue is detected (e.g., WAL corruption)."""
    pass