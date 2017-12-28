import logging
from multiprocessing import Process

from cassandra.util.message import MESSAGE_TYPES, ConnectionLostMessage
from cassandra.util.packing import recv_msg, addr_str_to_tuple
from cassandra.util.queue_item_types import *


class Receiver(Process):
    """
    Receiver: a process that receives message from a socket and put the message into a queue for further use.
    """
    def __init__(self, label, sock, addr, port, to_queue, connection_pool, listen_addr):
        super(Receiver, self).__init__()
        self.label = label
        self.sock = sock
        self.identifier = addr + ':' + str(port)
        self.to_queue = to_queue
        self.connection_pool = connection_pool
        self.listen_addr = listen_addr

    def run(self):

        logging.info('%s (%s) started' % (self.label, self.identifier))

        try:
            while True:
                msg = recv_msg(self.sock)
                if msg['code'] in MESSAGE_TYPES:
                    message = MESSAGE_TYPES[msg['code']](msg['data'], msg['source'])
                    self.to_queue.put({
                        'type': QUEUE_ITEM_TYPE_RECEIVED_MESSAGE,
                        'identifier': self.identifier,
                        'message': message})
                    logging.debug('%s (%s) received message %s' % (self.label, self.identifier, message))
                else:
                    logging.error('%s (%s) Unexpected message type: %s' % (self.label, self.identifier, msg['code']))

        except Exception as e:
            logging.error('%s (%s) error occurred - %s' % (self.label, self.identifier, e), exc_info=True)
            logging.info('%s (%s) removing connection from pool' % (self.label, self.identifier))
            server_name = self.connection_pool.get_server_name(self.identifier)
            identifier_tuple = addr_str_to_tuple(server_name)
            self.to_queue.put({
                'type': QUEUE_ITEM_TYPE_CONNECTION_LOST,
                'identifier': self.identifier,
                'message': ConnectionLostMessage(bytes(self.identifier, 'ascii'), identifier_tuple)})
            self.connection_pool.remove_connection(self.identifier)
