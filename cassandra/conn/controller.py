import logging
from multiprocessing import Manager, Lock, Process, Queue

from cassandra.util.packing import recv_msg, pack_msg_new_connection
from cassandra.util.queue_item_types import *
from cassandra.util.message_codes import *
from cassandra.util.message import MESSAGE_TYPES, ConnectionLostMessage


class Controller(Process):
    """ this controller receives messages from Receiver and spread them to subscribers """
    def __init__(self, label, from_queue, to_queue, connection_pool, bootstrapper_addr, listen_addr):
        super(Controller, self).__init__()
        self.label = label
        self.from_queue = from_queue
        self.to_queue = to_queue
        self.connection_pool = connection_pool
        self.bootstrapper_addr = bootstrapper_addr
        self.listen_addr = listen_addr
        self.registrations = {}
        self.queues = {}

    def register(self, regis_code, regis_iden):
        if regis_code not in self.registrations:
            self.registrations[regis_code] = []
        if regis_iden not in self.registrations[regis_code]:
            self.registrations[regis_code].append(regis_iden)
        if regis_iden not in self.queues:
            self.queues[regis_iden] = Queue()

        logging.debug('%s registered %s for code %d' % (self.label, regis_iden, regis_code))
        return self.queues[regis_iden]

    def spread_message(self, msg_code, identifier, message):
        for regis_iden in self.registrations.get(msg_code, []):
            self.queues[regis_iden].put({
                'type': msg_code,
                'identifier': regis_iden,
                'remote_identifier': identifier,
                'message': message})

    def run(self):
        logging.info('%s started - Pid %s' % (self.label, self.pid))

        # connect the bootstrapper server
        if self.bootstrapper_addr:
            self.to_queue.put({'type': QUEUE_ITEM_TYPE_NEW_CONNECTION, 'identifier': self.bootstrapper_addr})

        while True:
            item = self.from_queue.get()
            item_type = item['type']
            identifier = item['identifier']
            message = item['message']

            if item_type == QUEUE_ITEM_TYPE_RECEIVED_MESSAGE:
                msg_code = message.get_values()['code']

                if msg_code == MESSAGE_CODE_NEW_CONNECTION_HANDSHAKE:

                    source_addr, source_port = message.source_addr
                    addr_str = ':'.join([source_addr, str(source_port)])
                    self.connection_pool.update_connection(identifier, addr_str)

                    logging.debug('%s | update identifier %s for %s from handshake'
                                  % (self.label, addr_str, identifier))

                    message_data = pack_msg_new_connection(identifier)
                    msg_to_put = MESSAGE_TYPES[MESSAGE_CODE_NEW_CONNECTION](message_data['data'], message.source_addr)
                    self.spread_message(MESSAGE_CODE_NEW_CONNECTION, identifier, msg_to_put)

                else:
                    self.spread_message(msg_code, identifier, message)

            elif item_type == QUEUE_ITEM_TYPE_NEW_CONNECTION:
                self.spread_message(MESSAGE_CODE_NEW_CONNECTION, identifier, message)

            elif item_type == QUEUE_ITEM_TYPE_CONNECTION_LOST:
                self.spread_message(MESSAGE_CODE_CONNECTION_LOST, identifier, message)

            elif item_type == QUEUE_ITEM_TYPE_NOTIFICATION:
                self.spread_message(message.code, identifier, message)

            else:
                logging.error('%s unknown queue item type %d' % (self.label, item_type))
                continue
