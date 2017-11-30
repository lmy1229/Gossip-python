import logging
from multiprocessing import Process

from gossip.util.packing import recv_msg, pack_msg_new_connection
from gossip.util.queue_item_types import *
from gossip.util.message import MESSAGE_TYPES
from gossip.util.message_codes import *


class Receiver(Process):
    '''
    Receiver: a process that receives message from a socket and put the message into a queue for futher use.
    '''
    def __init__(self, label, sock, addr, port, to_queue, connection_pool):
        super(Receiver, self).__init__()
        self.label = label
        self.sock = sock
        self.identifier = addr + ':' + str(port)
        self.to_queue = to_queue
        self.connection_pool = connection_pool

    def run(self):
        
        logging.info('%s (%s) started' % (self.label, self.identifier))

        try:
            message_data = pack_msg_new_connection(self.identifier)
            self.to_queue.put({
                'type': QUEUE_ITEM_TYPE_NEW_CONNECTION, 
                'identifier': self.identifier, 
                'message': MESSAGE_TYPES[MESSAGE_CODE_NEW_CONNECTION](message_data['data'])})

            while True:
                msg = recv_msg(self.sock)
                # TODO: initialization of specific message type
                if msg['code'] in MESSAGE_TYPES:
                    message = MESSAGE_TYPES[msg['code']](msg['data'])
                else:
                    # undetected message type
                    pass
                self.to_queue.put({'type': QUEUE_ITEM_TYPE_RECEIVED_MESSAGE, 'identifier': self.identifier, 'message': message})
                logging.debug('%s (%s) received message %s' % (self.label, self.identifier, message))

        except Exception as e:
            logging.error('%s (%s) error occured - %s' % (self.label, self.identifier, e))
            logging.info('%s (%s) removing connection from pool' % (self.label, self.identifier))
            self.connection_pool.remove_connection(self.identifier)
            self.to_queue.put({'type': QUEUE_ITEM_TYPE_CONNECTION_LOST, 'identifier': self.identifier, 'message': None})

        