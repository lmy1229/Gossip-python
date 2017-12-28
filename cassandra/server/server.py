import logging
import time
from collections import Counter
from multiprocessing import Queue, Manager

from cassandra.conn.connection import ConnectionPool
from cassandra.conn.sender import Sender
from cassandra.conn.server import Server
from cassandra.server.controller import CassandraController
from cassandra.util.message import MESSAGE_CODE_NEW_CONNECTION, MESSAGE_CODE_CONNECTION_LOST, \
    MESSAGE_CODE_GOSSIP, MESSAGE_CODE_RESPONSE
from cassandra.util.packing import addr_str_to_tuple, addr_tuple_to_str
from cassandra.util import int_or_str
from cassandra.util.queue_item_types import QUEUE_ITEM_TYPE_RECEIVED_MESSAGE, QUEUE_ITEM_TYPE_SEND_MESSAGE
from cassandra.util.scheduler import Scheduler


class CassandraServer(Scheduler):
    """Server for Cassandra"""
    def __init__(self, node, partitioner, config, label='CassandraServer'):
        """ Explanation for some parameters:
        interval:           Time interval in seconds for response check task(default 1)
        response_timeout:   Timeout in seconds for send failed response to client(default 5)
        max_conn:           Max connection number for Connection Pool(default 5)
        response_protocol:  'any', 'all', or some integer.(default 'all')
        """

        self.manager = Manager()
        self.response_dict = self.manager.dict()  # map from (client address, json dumped request) to responses
        self.config = self.manager.dict()
        self.config['interval'] = int(config['interval'])
        self.config['response_timeout'] = int(config['response_timeout'])
        self.config['max_conn'] = int(config['max_connections'])
        self.config['response_protocol'] = int_or_str(config['response_protocol'])

        self.listen_addr = config['listen_addr']
        self.addr, self.port = addr_str_to_tuple(self.listen_addr)

        super(CassandraServer, self).__init__(self.config['interval'])

        self.label = label
        node.register(self.label, MESSAGE_CODE_RESPONSE)
        self.message_manager = node.get_manager(self.label)
        self.partitioner = partitioner

        self.sender_queue = Queue()
        self.receiver_queue = Queue()

        connection_pool = ConnectionPool(self.label+"(Pool)", self.config['max_conn'])

        self.server = Server(
            label=self.label+"(Server)",
            receiver_label=self.label+"(Receiver)",
            addr=self.addr,
            port=self.port,
            to_queue=self.receiver_queue,
            connection_pool=connection_pool,
            listen_addr=(self.addr, self.port),
            max_conn=self.config['max_conn'])

        self.sender = Sender(
            label=self.label+"(Sender)",
            receiver_label=self.label + "(Receiver)",
            from_queue=self.sender_queue,
            to_queue=self.receiver_queue,
            connection_pool=connection_pool,
            listen_addr=(self.addr, self.port))

        self.controller = CassandraController(
            label=self.label+"(Controller)",
            receiver_queue=self.receiver_queue,
            sender_queue=self.sender_queue,
            manager=self.message_manager,
            partitioner=self.partitioner,
            response_dict=self.response_dict,
            server_config=self.config,
        )

        self.server.start()
        self.sender.start()
        self.controller.start()

    def interval_task(self):
        for key in list(self.response_dict.keys()):
            request_time = self.response_dict[key]['request_time']
            if time.time() - request_time > self.config['response_timeout']:
                self.response_dict.pop(key)
                logging.warning('%s | Delete %s(Receive at %s) from response dict because of no response for long time'
                                % (self.label, key, request_time))

        # TODO delete these
        import random
        import string
        import mmh3
        import json
        from cassandra.util.message import RequestMessage

        def random_str(length):
            selection = string.ascii_letters + string.digits
            return ''.join([random.choice(selection) for _ in range(length)])

        client_addr = '0.0.0.0:7111'
        key, value = random_str(4), random_str(4)
        logging.debug('Virtual Client | send request, key=%s, value=%s' % (key, value))

        def send_request(request):
            request_hash = mmh3.hash(json.dumps((client_addr, request)))
            d = {'request': request, 'request_hash': request_hash}
            s = bytes(json.dumps(d), 'ascii')
            message = RequestMessage(s, client_addr)
            self.receiver_queue.put({
                'type': QUEUE_ITEM_TYPE_RECEIVED_MESSAGE,
                'identifier': client_addr,
                'message': message
            })

        send_request(['put', key, value])
        send_request(['get', key])

    def main_task(self):
        try:
            while True:
                item = self.message_manager.get_msg()
                item_type = item['type']
                msg = item['message']

                logging.debug('*'*60)
                logging.debug(msg)
                logging.debug(item)
                logging.debug('-'*60)

                # TODO, check with lmy
                if item_type == MESSAGE_CODE_RESPONSE:

                    if msg.code == MESSAGE_CODE_RESPONSE:
                        self.process_response(msg)

                    else:
                        logging.error('%s | Unknown message code %d' % (self.label, msg.code))

                else:
                    logging.error('%s | Unknown queue item type %d' % (self.label, item_type))

                    logging.debug('*'*60)
                    logging.debug(item)
                    logging.debug(msg)
                    logging.debug('-'*60)

        except Exception as e:
            logging.error("Cassandra error: {}".format(e), exc_info=True)
            exit(1)

    def can_response(self, responses):
        str_to_threshold = {'any': 1, 'all': self.partitioner.v_node_num}
        protocol = self.config['response_protocol']
        threshold = str_to_threshold[protocol] if isinstance(protocol, str) else protocol

        response_length = sum(1 for _ in filter(None.__ne__, responses.values()))
        return response_length >= threshold

    def process_response(self, msg):
        if msg.request_hash not in self.response_dict:
            logging.warning('%s | Ignore response because Request hash from %s not in response dict: %s'
                            % (self.label, node_addr, msg.get_values()))
            return

        responses = self.response_dict[msg.request_hash]['responses']

        node_addr = addr_tuple_to_str(msg.source_addr)
        if responses[node_addr] is not None:
            logging.error('%s | Error occurred, Response of request hash from %s is not None: before(%s), this(%s)'
                          % (self.label, node_addr, responses[node_addr].get_values(), msg.get_values()))
            return

        assert node_addr in responses

        responses[node_addr] = msg

        if self.can_response(responses):
            c = Counter(filter(None.__ne__, responses.values()))
            msg_to_send = c.most_common(1)[0][0]
            msg_to_send.source_addr = self.listen_addr
            client_addr = self.response_dict[msg_to_send.request_hash]['client_addr']

            self.sender_queue.put({
                'type': QUEUE_ITEM_TYPE_SEND_MESSAGE,
                'identifier': client_addr,
                'message': msg_to_send
            })
            self.response_dict.pop(msg_to_send.request_hash)

            logging.info('%s | Sending response of request %s to %s with status %s'
                         % (self.label, msg.request_hash, client_addr, msg.status))
