import logging
import sys
from queue import Empty
from multiprocessing import Queue


from gossip.conn.connection import ConnectionPool
from gossip.conn.server import Server
from gossip.conn.controller import Controller
from gossip.conn.sender import Sender
from gossip.util.message import MESSAGE_TYPES
from gossip.util.config_parser import read_config
from gossip.util.packing import pack_msg_new_connection
from gossip.util.queue_item_types import *
from gossip.util.message_codes import *


class Node():
    def __init__(self, config_path, **kwargs):
        self.config_path = config_path
        self.config = read_config(config_path)

        max_connections = self.config.get('max_connections', 5)
        listen_addr = self.config.get('listen_addr')
        addr, port = listen_addr.split(':')
        port = int(port)
        bootstrap_addr = self.config.get('bootstrapper', None)

        connection_pool = self.connection_pool = ConnectionPool('pool', self.config['max_connections'])

        self.config['controller_label'] = controller_label = kwargs.get('label', 'Controller')
        self.config['receiver_label'] = receiver_label = kwargs.get('receiver_label', 'Receiver')
        self.config['sender_label'] = sender_label = kwargs.get('sender_label', 'Sender')

        sender_queue = self.sender_queue = Queue()
        receiver_queue = self.receiver_queue = Queue()
        self.server = Server('server', 'receiver', addr, port, receiver_queue, connection_pool, max_connections)
        self.controller = Controller(controller_label, receiver_queue, sender_queue, connection_pool, bootstrap_addr)
        self.sender = Sender(sender_label, receiver_label, sender_queue, receiver_queue, connection_pool)

        self.queues = {}

    def start(self):
        self.server.start()
        self.controller.start()
        self.sender.start()

        self.server.join()
        self.controller.join()
        self.sender.join()

        exit_codes = self.server.exitcode | self.controller.exitcode | self.sender.exitcode

        if exit_codes > 0:
            logging.error('Node exited with error code %d' % exit_codes)
        else:
            logging.info('Node exited with return code %d' % exit_codes)

        sys.exit(exit_codes)

    def register(self, identifier, code):

        q = self.controller.register(code, identifier)
        if identifier not in self.queues:
            self.queues[identifier] = q

    def get_manager(self, identifier):

        queue = self.queues.get(identifier, None)
        manager = NodeManager(identifier, self.sender_queue, self.receiver_queue, queue, self.config['listen_addr'], self.config['seeds'])
        return manager

class NodeManager():
    def __init__(self, identifier, sender_queue, receiver_queue, message_queue, addr, seeds):
        self.identifier = identifier
        self.sender_queue = sender_queue
        self.receiver_queue = receiver_queue
        self.message_queue = message_queue
        self.addr = addr
        self.seeds = seeds

    def connect(self, remote_identifier):
        message_data = pack_msg_new_connection(remote_identifier)
        self.sender_queue.put({
            'type': QUEUE_ITEM_TYPE_NEW_CONNECTION,
            'identifier': remote_identifier,
            'message': MESSAGE_TYPES[MESSAGE_CODE_NEW_CONNECTION](message_data['data'])})

    def send_gossip_msg(self, remote_identifier, msg):
        message = MESSAGE_TYPES[MESSAGE_CODE_GOSSIP](msg)
        self.sender_queue.put({
            'type': QUEUE_ITEM_TYPE_SEND_MESSAGE,
            'identifier': remote_identifier,
            'message': message
            })

    def send_notification(self, msg):
        # used for one app(e.g. Gossip) to notify another app(e.g. data management?)
        # msg has to be a Message object!!!
        self.receiver_queue.put({
            'type': QUEUE_ITEM_TYPE_NOTIFICATION,
            'identifier': self.identifier,
            'message': msg
            })

    def get_msg(self):
        try:
            return self.message_queue.get(block=True)
        except Empty:
            return None

    def get_self_identifier(self):
        return self.identifier

    def get_self_addr(self):
        return self.addr

    def get_seeds(self):
        return self.seeds
