import logging
from multiprocessing import Manager, Lock, Process, Queue

from gossip.util.queue_item_types import *
from gossip.util.message_codes import *

# class RegistrationHandler():
#     """ a handler for registrations. """
#     def __init__(self):
#         self.registrations = Manager().dict()
#         self.lock = Lock()

#     def register(self, code, identifier):
        
#         self.lock.acquire()
#         if code not in self.registrations:
#             self.registrations[code] = []
#         if identifier not in self.registrations[code]:
#             self.registrations[code].append(identifier)
#         self.lock.release()

#     def unregister(self, code, identifier):
        
#         

class Controller(Process):
    """ this controller receives messages from Receiver and spread them to subscribers """
    def __init__(self, label, from_queue, to_queue, connection_pool, bootstrapper_addr):
        super(Controller, self).__init__()
        self.label = label
        self.from_queue = from_queue
        self.to_queue = to_queue
        self.connection_pool = connection_pool
        self.bootstrapper_addr = bootstrapper_addr
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

                if msg_code == MESSAGE_CODE_GOSSIP:
                    # spread gossip message to upper applications
                    if MESSAGE_CODE_GOSSIP not in self.registrations:
                        continue
                    for regis_iden in self.registrations[MESSAGE_CODE_GOSSIP]:
                        self.queues[regis_iden].put({'type': MESSAGE_CODE_GOSSIP, 'identifier': regis_iden, 'message': message })

            if item_type == QUEUE_ITEM_TYPE_NEW_CONNECTION:
                # spread new connection message to all subscribers
                if MESSAGE_CODE_NEW_CONNECTION not in self.registrations:
                    continue
                for regis_iden in self.registrations[MESSAGE_CODE_NEW_CONNECTION]:
                    self.queues[regis_iden].put({'type': MESSAGE_CODE_NEW_CONNECTION, 'identifier': regis_iden, 'message': message})

            elif item_type == QUEUE_ITEM_TYPE_CONNECTION_LOST:
                # spread connection lost message to all subscribers
                if MESSAGE_CODE_CONNECTION_LOST not in self.registrations:
                    continue
                for regis_iden in self.registrations[MESSAGE_CODE_CONNECTION_LOST]:
                    self.queues[regis_iden].put({'type': MESSAGE_CODE_CONNECTION_LOST, 'identifier': regis_iden, 'message': message})

            else:
                logging.error('%s unknown queue item type' % self.label)
                continue
        