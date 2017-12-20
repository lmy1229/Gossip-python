import logging
import sys
from queue import Empty
from multiprocessing import Queue


from cassandra.conn.connection import ConnectionPool
from cassandra.conn.server import Server
from cassandra.conn.controller import Controller
from cassandra.conn.sender import Sender
from cassandra.util.message import MESSAGE_TYPES
from cassandra.util.config_parser import read_config
from cassandra.util.packing import pack_msg_new_connection
from cassandra.util.queue_item_types import *
from cassandra.util.message_codes import *


class Node():
    def __init__(self, config_path, **kwargs):
        self.config_path = config_path
        self.config = read_config(config_path)

        max_connections = self.config.get('max_connections', 5)
        listen_addr = self.config.get('listen_addr')
        addr, port = listen_addr.split(':')
        port = int(port)
        self.listen_addr = (addr, port)
        bootstrap_addr = self.config.get('bootstrapper', None)

        connection_pool = self.connection_pool = ConnectionPool('pool', self.config['max_connections'])

        self.config['controller_label'] = controller_label = kwargs.get('label', 'Controller')
        self.config['receiver_label'] = receiver_label = kwargs.get('receiver_label', 'Receiver')
        self.config['sender_label'] = sender_label = kwargs.get('sender_label', 'Sender')

        sender_queue = self.sender_queue = Queue()
        receiver_queue = self.receiver_queue = Queue()
        self.server = Server('server', 'receiver', addr, port, receiver_queue, connection_pool, self.listen_addr, max_connections)
        self.controller = Controller(controller_label, receiver_queue, sender_queue, connection_pool, bootstrap_addr, self.listen_addr)
        self.sender = Sender(sender_label, receiver_label, sender_queue, receiver_queue, connection_pool, self.listen_addr)

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
        manager = NodeManager(identifier, self.sender_queue, self.receiver_queue, queue, self.listen_addr, self.config['seeds'])
        return manager

class NodeManager():
    def __init__(self, identifier, sender_queue, receiver_queue, message_queue, listen_addr, seeds):
        self.identifier = identifier
        self.sender_queue = sender_queue
        self.receiver_queue = receiver_queue
        self.message_queue = message_queue
        self.listen_addr = listen_addr
        self.seeds = seeds

    def connect(self, remote_identifier):
        message_data = pack_msg_new_connection(remote_identifier)
        self.sender_queue.put({
            'type': QUEUE_ITEM_TYPE_NEW_CONNECTION,
            'identifier': remote_identifier,
            'message': MESSAGE_TYPES[MESSAGE_CODE_NEW_CONNECTION](message_data['data'], self.listen_addr)})

    def send_gossip_msg(self, remote_identifier, bmsg):
        message = MESSAGE_TYPES[MESSAGE_CODE_GOSSIP](bmsg, self.listen_addr)
        self.sender_queue.put({
            'type': QUEUE_ITEM_TYPE_SEND_MESSAGE,
            'identifier': remote_identifier,
            'message': message
            })

    def send_msg_object(self, remote_identifier, omsg):
        self.sender_queue.put({
            'type': QUEUE_ITEM_TYPE_SEND_MESSAGE,
            'identifier': remote_identifier,
            'message': omsg
            })

    def send_notification(self, msg):
        # used for one app(e.g. Gossip) to notify another app(e.g. data management?)
        # msg has to be a Message object!!!
        self.receiver_queue.put({
            'type': QUEUE_ITEM_TYPE_NOTIFICATION,
            'identifier': self.identifier,
            'message': msg
            })

    def get_msg(self, block=True):
        try:
            return self.message_queue.get(block=block)
        except Empty:
            return None

    def get_self_identifier(self):
        return self.identifier

    def get_self_addr(self):
        return self.listen_addr

    def get_seeds(self):
        return self.seeds
