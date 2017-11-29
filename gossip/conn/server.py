import logging
import multiprocessing
import socket

from gossip.conn.receiver import Receiver

class GossipServer(multiprocessing.Process):
    def __init__(self, label, receiver_label, addr, port, to_queue, connection_pool, max_conn=5):
        super(GossipServer, self).__init__()
        self.label = label
        self.addr = addr
        self.port = port
        self.to_queue = to_queue
        self.connection_pool = connection_pool
        self.max_conn = max_conn

    def run(self):
        ''' listening on the port, create receiver. '''
        try:
            logging.info('%s | start (%s:%d) - Pid: %s' % (self.label, self.addr, self.port, self.pid))
            socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            socket.bind((self.addr, self.port))
            socket.listen(self.max_conn)

            while True:
                client_socket, address = socket.accept()
                addr, port = addr
                identifier = addr + ':' + str(port)
                self.connection_pool.add_connection(identifier, client_socket, server_name=identifier)
                logging.info('%s | accepted a new connection from %s' % (self.label, identifier))
                receiver = Receiver(receiver_label, client_socket, addr, port, self.to_queue, self.connection_pool)
                receiver.start()
            socket.close()
        except Exception as e:
            logging.error('%s | crashed (%s:%d) - Pid: %s - %s' % (self.label, self.addr, self.port, self.pid, e))
