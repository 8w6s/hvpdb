class HVPError(Exception):
    pass

class AuthError(HVPError):
    pass

class ConsistencyError(HVPError):
    pass