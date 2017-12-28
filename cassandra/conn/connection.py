import logging
from multiprocessing import Manager, Lock

from cassandra.util.exceptions import IdentifierNotFoundException


class ConnectionPool:
    """A pool for connections."""

    def __init__(self, label, size):
        self.label = label
        self.size = size
        self.lock = Lock()
        self.manager = Manager()
        self.connections = self.manager.dict()
        self.alias = self.manager.dict()

    def add_connection(self, remote_addr, socket, identifier=None):
        """ add a new connection with identifier and socket """
        self.lock.acquire()
        if remote_addr not in self.connections:
            self.connections[remote_addr] = {'socket': socket, 'identifier': identifier}
            if identifier:
                if identifier in self.alias:
                    logging.error('%s | identifier %s (for %s) already used by %s' % (
                        self.label, identifier, remote_addr, self.alias[identifier]))
                else:
                    self.alias[identifier] = remote_addr
            logging.debug('%s | added a new connection %s' % (self.label, identifier))
            self.lock.release()
        else:
            self.lock.release()
            logging.debug('%s | connection %s already exists' % (self.label, remote_addr))

    def remove_connection(self, remote_addr):
        """ remove a connection by identifier """
        self.lock.acquire()
        if remote_addr in self.connections:
            removed = self.connections.pop(remote_addr, None)
            for k, v in self.alias.items():
                if v == remote_addr:
                    del self.alias[k]
            logging.debug('%s | connection %s removed' % (self.label, remote_addr))
            self.lock.release()
            return removed
        else:
            logging.debug('%s | connection %s does not exist' % (self.label, remote_addr))
            self.lock.release()
            return None

    def get_connection_by_remote_addr(self, remote_addr):
        """ get the socket of a connection by identifier or remote_addr """
        self.lock.acquire()
        conn = self.connections.get(remote_addr, None)
        self.lock.release()

        if conn:
            return conn['socket']
        else:
            logging.error('%s | get connection %s failed because it does not exist' % (self.label, remote_addr))
            raise IdentifierNotFoundException(remote_addr)

    def get_connection(self, name):

        remote_addr = self.get_remote_addr_by_identifier(name) or name

        return self.get_connection_by_remote_addr(remote_addr)

    def get_remote_addr_by_identifier(self, identifier):
        self.lock.acquire()
        if identifier in self.alias:
            remote_addr = self.alias[identifier]
            self.lock.release()
            return remote_addr
        else:
            self.lock.release()
            return None

    def update_connection(self, remote_addr, identifier):
        """ update the identifier of the connection """
        self.lock.acquire()
        if remote_addr in self.connections:

            # test if identifier already in use
            if identifier in self.alias:
                logging.error('%s | identifier %s (for %s) already used by %s' % (
                    self.label, identifier, remote_addr, self.alias[identifier]))
                self.lock.release()
                return
            # update alias
            self.alias[identifier] = remote_addr

            # update connections
            sock = self.connections[remote_addr]['socket']
            self.connections[remote_addr] = {'socket': sock, 'identifier': identifier}
            self.lock.release()
            logging.debug('%s | connection %s has updated to %s' % (self.label, remote_addr, identifier))
        else:
            logging.debug('%s | connection %s cannot be updated because it does not exist' % (self.label, remote_addr))
            self.lock.release()

    def get_server_name(self, identifier):
        """ get the server name of a connection by its identifier """
        self.lock.acquire()
        conn = self.connections.get(identifier, None)
        self.lock.release()

        if conn:
            return conn['identifier']
        else:
            logging.debug('%s | get server identifier for %s failed because it does not exist' % (self.label, identifier))
            raise IdentifierNotFoundException(identifier)

    def get_identifiers(self):
        """ get all connection identifiers in this pool """
        self.lock.acquire()
        identifiers = self.connections.keys()
        self.lock.release()

        return identifiers
