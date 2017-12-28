import json
import logging
import mmh3
import time
from multiprocessing import Process

from cassandra.util.message import RequestMessage, ResponseMessage
from cassandra.util.message_codes import MESSAGE_CODE_REQUEST
from cassandra.util.queue_item_types import QUEUE_ITEM_TYPE_RECEIVED_MESSAGE, QUEUE_ITEM_TYPE_SEND_MESSAGE


class CassandraController(Process):
    """ this controller receives messages from client and process it """

    def __init__(self, label, receiver_queue, sender_queue, manager, partitioner, response_dict, server_config):
        super(CassandraController, self).__init__()
        self.label = label

        # Sender queue and receiver queue for client
        self.receiver_queue = receiver_queue
        self.sender_queue = sender_queue

        # Node message service for other nodes
        self.manager = manager

        # partitioner for get addresses of nodes
        self.partitioner = partitioner

        # variable shared with server
        self.response_dict = response_dict
        self.server_config = server_config

    def run(self):
        """ Receive request from client, process it (and maybe send response).

        Supported request types:
        1. [get key]: get value of key.
        2. [put key value]: put (key, value) to database.
        3. [set key value]: set configuration of key to value.
        """
        try:
            logging.info('%s | start - Pid: %s' % (self.label, self.pid))

            while True:
                item = self.receiver_queue.get()
                server_addr = item['identifier']
                item_type = item['type']

                if item_type == QUEUE_ITEM_TYPE_RECEIVED_MESSAGE:
                    message = item['message'].get_values()
                    request = message['request']
                    code = message['code']
                    client_addr = message['source']
                    request_hash = message['request_hash']

                    assert code == MESSAGE_CODE_REQUEST, 'Request code %d != %d' % (code, MESSAGE_CODE_REQUEST)

                    logging.info('%s | processing commands: %s from %s' % (self.label, request_hash, client_addr))

                    status, resp = False, None
                    if request[0] in ['get', 'put']:
                        key = request[1]

                        hash1 = mmh3.hash((client_addr, request))
                        assert hash1 == request_hash, 'Request hash %s != %s' % (request_hash, hash1)

                        # send request to nodes
                        s = json.dumps({'request': request, 'request_hash': request_hash})
                        msg = RequestMessage(bytes(s, 'ascii'), self.manager.get_self_addr())
                        dst_addrs = self.partitioner.get_node_addrs(key)
                        for addr in dst_addrs:
                            self.manager.send_msg_object(addr, msg)

                        logging.info('%s | Send request(%s) to nodes(%s)' % (self.label, request_hash, dst_addrs))

                        self.response_dict[request_hash] = {
                            'responses':    {addr: None for addr in dst_addrs},
                            'request_time': time.time(),
                            'client_addr': client_addr
                        }

                        logging.info('%s | Record %s to response dict' % (self.label, request_hash))

                    elif message[0] == 'set':
                        status, resp = self.set(request[1], request[2])

                    else:
                        status = False
                        resp = 'unknown commands type %s from %s' % (str(request), client_addr)
                        logging.error('%s | %s' % (self.label, resp))

                    if resp is not None:
                        d = {'status': status, 'description': resp, 'request_hash': request_hash}
                        s = bytes(json.dumps(d), 'ascii')
                        msg_to_send = ResponseMessage(s, server_addr)

                        self.sender_queue.put({
                            'type': QUEUE_ITEM_TYPE_SEND_MESSAGE,
                            'identifier': client_addr,
                            'message': msg_to_send,
                        })

                else:
                    logging.error('%s | unknown queue item type %d' % (self.label, item_type))
                    continue

        except Exception as e:
            logging.error('%s | crashed Pid: %s - %s' % (self.label, self.pid, e))

    def set(self, key, value):
        """ Configure setting """
        # TODO: Now I just set the value of key in config. Not interrupt running process.
        # TODO: check type and range of value

        try:
            self.server_config[key] = value
            return True, ''
        except Exception as e:
            error_message = 'Error occurred when set key(%s) to value(%s) into database: %s' % (key, value, e)
            logging.error('%s | %s' % (self.label, error_message))
            return False, error_message
