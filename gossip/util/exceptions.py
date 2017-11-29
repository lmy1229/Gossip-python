class IdentifierNotFoundException(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        
class ConnectionLostException(Exception):
    def __init__(self, msg):
        super().__init__(msg)

