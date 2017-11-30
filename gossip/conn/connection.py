import logging
from multiprocessing import Manager, Lock

from gossip.util.exceptions import IdentifierNotFoundException

class ConnectionPool():
    """A pool for connections."""

    def __init__(self, label, size):
        self.label = label
        self.size = size
        self.lock = Lock()
        self.connections = Manager().dict()
    
    def add_connection(self, identifier, socket, server_name=None):
        ''' add a new connection with identifier and socket '''
        self.lock.acquire()
        if identifier not in self.connections:
            self.connections[identifier] = {'socket': socket, 'name': server_name}
            logging.debug('%s | added a new connection %s' % (self.label, identifier))
            self.lock.release()
        else: 
            self.lock.release()
            logging.debug('%s | connection %s already exists' % (self.label, identifier))

    def remove_connection(self, identifier):
        ''' remove a connection by identifier '''
        self.lock.acquire()
        if identifier in self.connections:
            removed = self.connections.pop(identifier, None)
            logging.debug('%s | connection %s removed' % (self.label, identifier))
            self.lock.release()
            return removed
        else:
            logging.debug('%s | connection %s does not exist' % (self.label, identifier))
            self.lock.release()
            return None

    def get_connection(self, identifier):
        ''' get the socket of a connection by it's identifier '''
        self.lock.acquire()
        conn = self.connections.get(identifier, None)
        self.lock.release()

        if conn:
            return conn['socket']
        else:
            logging.debug('%s | get connection %s failed because it does not exist' % (self.label, identifier))
            raise IdentifierNotFoundException(identifier)

    def update_connection(self, identifier, server_name):
        ''' update the server name of the connection '''
        self.lock.acquire()
        if identifier in self.connections:
            sock = self.connections[identifier]['socket']
            self.connections[identifier] = {'socket': sock, 'name': server_name}
            self.lock.release()
            logging.debug('%s | connection %s has updated to %s' % (self.label, identifier, server_name))
        else:
            logging.debug('%s | connection %s cannot be updated becauser it does not exist' % (self.label, identifier))
            self.lock.release()

    def get_server_name(self, identifier):
        ''' get the server name of a connection by its identifier '''
        self.lock.acquire()
        conn = self.connections.get(identifier, None)
        self.lock.release()

        if conn:
            return conn['name']
        else:
            logging.debug('%s | get server name for %s failed because it does not exist' % (self.label, identifier))
            raise IdentifierNotFoundException(identifier)

    def get_identifiers(self):
        ''' get all connection identifiers in this pool '''
        self.lock.acquire()
        identifiers = self.connections.keys()
        self.lock.release()

        return identifiers
