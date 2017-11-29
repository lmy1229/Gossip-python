import logging
from multiprocessing import Process


class Receiver(Process):
    '''
    Receiver: a process that receives message from a socket and put the message into a queue for futher use.
    '''
    def __init__(self, label, sock, addr, port, to_queue, connection_pool):
        super(Receiver, self).__init__()
        self.label = label
        self.sock = sock
        self.identifier = addr + ':' + str(port)
        self.to_queue = to_queue
        self.connection_pool = connection_pool

    
        