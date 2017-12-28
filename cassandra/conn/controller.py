import logging
from multiprocessing import Process, Queue

from cassandra.util.message import MESSAGE_TYPES, NewConnectionMessage
from cassandra.util.message_codes import *
from cassandra.util.packing import pack_msg_new_connection, addr_str_to_tuple
from cassandra.util.queue_item_types import *


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

    def register(self, register_code, register_identifier):
        if register_code not in self.registrations:
            self.registrations[register_code] = []
        if register_identifier not in self.registrations[register_code]:
            self.registrations[register_code].append(register_identifier)
        if register_identifier not in self.queues:
            self.queues[register_identifier] = Queue()

        logging.debug('%s registered %s for code %d' % (self.label, register_identifier, register_code))
        return self.queues[register_identifier]

    def spread_message(self, msg_code, identifier, message):
        for register_identifier in self.registrations.get(msg_code, []):
            self.queues[register_identifier].put({
                'type': msg_code,
                'identifier': register_identifier,
                'remote_identifier': identifier,
                'message': message})

    def run(self):
        logging.info('%s started - Pid %s' % (self.label, self.pid))

        # connect the bootstrapper server
        if self.bootstrapper_addr:
            message_data = pack_msg_new_connection(self.bootstrapper_addr)
            msg_to_put = NewConnectionMessage(message_data['data'], self.listen_addr)
            self.to_queue.put({'type': QUEUE_ITEM_TYPE_NEW_CONNECTION, 'identifier': self.bootstrapper_addr, 'message': msg_to_put})

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
