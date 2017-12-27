from cassandra.util.scheduler import Scheduler
from cassandra.conn.server import Server
from cassandra.conn.sender import Sender
from multiprocessing import Process, Queue
from cassandra.partitioner.ring_partitioner import RingPartitioner
from cassandra.util.message import GetRequestMessage, PutRequestMessage
from cassandra.util.packing import successed_message, failed_message
import logging
import socket
import json


class CassandraController(Process):
    """ this controller receives messages from client and process it """

    def __init_(self, label, receiver_queue, sender_queue, manager, partitioner):
        self.label = label

        # Sender queue and receiver queue for client
        self.receiver_queue = receiver_queue
        self.sender_queue = sender_queue

        # Node message Servise for other nodes
        self.manager = manager

        # partitioner for get addresses of nodes
        self.partitioner = partitioner

    def run(self):
        """ Receive commands from client, process it and send response.

        Supported command types:
        1. [get key]: get value of key.
        2. [put key value]: put (key, value) to database.
        3. [set key value]: set config of key to value.
        """
        try:
            logging.info('%s | start - Pid: %s' % (self.label, self.addr, self.port, self.pid))

            while True:
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
        ''' Only implemented "no" protocol now.
        '''
        msg = GetRequestMessage(bytes(key, 'ascii'))
        dst_addrs = self.partitioner.get_node_addrs(key)
        self.send_messages_to_nodes(msg, dst_addrs=)
        return successed_message()

    def put(self, key, value):
        s = json.dumps({'key': key, 'value': value})
        msg = PutRequestMessage(bytes(s, 'ascii'))
        dst_addrs = self.partitioner.get_node_addrs(key)
        self.send_messages_to_nodes(msg, dst_addrs)
        return successed_message()

    def send_messages_to_nodes(self, msg, dst_addrs):
        for addr in dst_addrs:
            self.manager.send_msg_object(addr, msg)

    # TODO
    def set(self, key, value):
        msg = 'Set commands is not supported yet!'
        return failed_message(msg)
