class IdentifierNotFoundException(Exception):
    def __init__(self, msg):
        super(IdentifierNotFoundException, self).__init__(msg)


class ConnectionLostException(Exception):
    def __init__(self, msg):
        super(ConnectionLostException, self).__init__(msg)
