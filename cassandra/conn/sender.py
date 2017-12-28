import logging
import multiprocessing
import socket

from cassandra.util.queue_item_types import *
from cassandra.conn.receiver import Receiver
from cassandra.util.exceptions import IdentifierNotFoundException
from cassandra.util.message import NewConnectionHandShakeMessage, NewConnectionMessage
from cassandra.util.packing import addr_tuple_to_str, addr_str_to_tuple
import sys
import traceback


class Sender(multiprocessing.Process):
    """ Sender: send message from a queue or establish a new connection """
    def __init__(self, label, receiver_label, from_queue, to_queue, connection_pool, listen_addr):
        super(Sender, self).__init__()
        self.label = label
        self.receiver_label = receiver_label
        self.from_queue = from_queue
        self.to_queue = to_queue
        self.connection_pool = connection_pool
        self.listen_addr = listen_addr
        self.receiver_counter = 0

    def run(self):

        logging.info('%s started - Pid: %ds' % (self.label, self.pid))
        while True:
            item = self.from_queue.get()
            item_type = item['type']
            item_identifier = item['identifier']

            if item_type == QUEUE_ITEM_TYPE_SEND_MESSAGE:
                message = item['message']
                if not message.source_addr:
                    message.source_addr = self.listen_addr
                # send to myself
                if item_identifier == addr_tuple_to_str(self.listen_addr):
                    self.to_queue.put({
                        'type': QUEUE_ITEM_TYPE_RECEIVED_MESSAGE,
                        'identifier': item_identifier,
                        'message': message})
                    continue
                # send to other server
                try:
                    connection = self.connection_pool.get_connection(item_identifier)
                except IdentifierNotFoundException:
                    logging.error('%s | connection %s not found' % (self.label, item_identifier))
                    continue

                if connection and message:
                    try:
                        data = message.encode()
                        connection.send(data)
                        logging.debug('%s | sent message (type %d) to client %s - %s'
                                      % (self.label, message.code, item_identifier, message.data))
                    except Exception as e:
                        print(e)
                        print("Exception in user code:")
                        print('-' * 60)
                        traceback.print_exc(file=sys.stdout)
                        print('-' * 60)

                        self.connection_pool.remove_connection(item_identifier)
                        logging.error('%s | connection %s lost' % (self.label, item_identifier))

            elif item_type == QUEUE_ITEM_TYPE_NEW_CONNECTION:
                logging.info('%s | establishing new connection to %s' % (self.label, item_identifier))

                recv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                addr, port = item_identifier.split(':')
                port = int(port)
                try:
                    recv_socket.connect((addr, port))
                    self.connection_pool.add_connection(item_identifier, recv_socket, item_identifier)
                    logging.info('%s | adding connection %s to connection pool' % (self.label, item_identifier))

                    # send a new_connection message
                    message_data = pack_msg_new_connection(item_identifier)
                    msg_to_put = NewConnectionMessage(message_data['data'], addr_str_to_tuple(item_identifier))
                    self.to_queue.put({'type': QUEUE_ITEM_TYPE_NEW_CONNECTION, 'identifier': item_identifier, 'message': msg_to_put})

                    # send a handshake message to remote
                    hs_message = NewConnectionHandShakeMessage(0, self.listen_addr)
                    recv_socket.send(hs_message.encode())
                    logging.debug('%s | sent handshake message (type %d) to client %s'
                                  % (self.label, hs_message.code, item_identifier))

                except Exception as e:
                    logging.error('%s | Connection error: %s' % (self.label, e))
                    print("Exception in user code:")
                    print('-' * 60)
                    traceback.print_exc(file=sys.stdout)
                    print('-' * 60)
                    continue

                # create receiver for new connection
                receiver = Receiver(
                    self.receiver_label,
                    recv_socket,
                    addr,
                    port,
                    self.to_queue,
                    self.connection_pool,
                    self.listen_addr)
                receiver.start()
                self.receiver_counter = self.receiver_counter + 1

            else:
                # unrecognized message type
                logging.error('%s | Unrecognized message type' % self.label)
                raise Exception('Unrecognized message type')
