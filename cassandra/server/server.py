from cassandra.util.scheduler import Scheduler
from cassandra.conn.server import Server
from cassandra.conn.sender import Sender
from multiprocessing import Process, Queue
from cassandra.partitioner.ring_partitioner import RingPartitioner
import logging
import socket
import json


class CassandraServer(Process):
    """Server for Cassandra"""
    def __init__(self, label, addr, port, manager, partitioner, max_conn=5):
        super(CassandraServer, self).__init__()
        self.addr = addr
        self.port = port
        self.label = label
        self.manager = manager
        self.partitioner = partitioner

        self.sender_queue = Queue()
        self.receiver_queue = Queue()

        connection_pool = ConnectionPool(self.label+"(Pool)", self.max_conn)

        self.server = Server(
            label=self.label+"(Server)",
            receiver_label=self.label+"(Receiver)",
            addr=self.addr,
            port=self.port,
            to_queue=self.receiver_queue,
            connection_pool=connection_pool,
            listen_addr=(self.addr, self.port),
            max_conn=max_conn)

        self.sender = Sender(
            label=self.label+"(Sender)",
            reciever_label=self.label+"(Receiver)",
            from_queue=self.sender_queue,
            to_queue=self.receiver_queue,
            connection_pool=connection_pool,
            listen_addr=(self.addr, self.port))

    def run(self):
        """ Receive commands from client, process it and send response.

        Supported command types:
        1. [get key]: get value of key.
        2. [put key value]: put (key, value) to database.
        3. [set key value]: set config of key to value.
        """
        try:
            logging.info('%s | start (%s:%d) - Pid: %s' % (self.label, self.addr, self.port, self.pid))

            self.server.start()
            self.sender.start()

            item = self.receiver_queue.get()
            remote_identifier = item['identifier']
            item_type = item['type']

            # TODO other connection message like new connection?
            if item_type == QUEUE_ITEM_TYPE_RECEIVED_MESSAGE:
                message = json.loads(item['message'])

                logging.error('%s | processing commands: %s from %s' % (item['message'], remote_identifier))

                # TODO not iterative?
                if message[0] == 'get':
                    resp = self.get(message[1])
                elif message[0] == 'put':
                    resp = self.put(message[1], message[2])
                elif message[0] == 'set':
                    resp = self.set(message[1], message[2])
                else:
                    error_message = 'unknown commands type %s from %s' % (self.label, message[0], remote_identifier)
                    logging.error('%s ' + resp)
                    resp = self.failed_message(error_message)

                self.sender_queue.put({
                    'type': QUEUE_ITEM_TYPE_SEND_MESSAGE,
                    'identifier': remote_identifier,
                    'message': resp,
                })

            else:
                logging.error('%s unknown queue item type %d' % (self.label, item_type))
                continue

        except Exception as e:
            logging.error('%s | crashed (%s:%d) - Pid: %s - %s' % (self.label, self.addr, self.port, self.pid, e))

    def get(self, key):
        '''Only implement "no" protocol
        '''
        self.send_messages(msg=RouteRequestMessage(bytes(key, 'ascii')),
                           dst_addrs=self.partitioner.get_node_addrs(key))
        return self.successed_message()

    def put(self, key, value):
        s = json.dumps({'key': key, 'value': value})
        self.send_messages(msg=RouteDataMessage(bytes(s, 'ascii')),
                           dst_addrs=self.partitioner.get_node_addrs(key))
        return self.successed_message()

    def send_messages(self, msg, dst_addrs):
        for addr in dst_addrs:
            self.sender_queue.put({
                'type': QUEUE_ITEM_TYPE_SEND_MESSAGE,
                'identifier': addr,
                'message': msg
            })

    def set(self, key, value):
        msg = 'Set commands is not supported yet!'
        return self.failed_message(msg)

    @staticmethod
    def failed_message(msg=''):
        return {
            'status': 'Failed',
            'message': msg
        }

    @staticmethod
    def successed_message(msg=''):
        return {
            'status': 'Success',
            'message': msg
        }
