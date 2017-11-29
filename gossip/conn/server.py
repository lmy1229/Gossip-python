import logging
import multiprocessing
import socket

class GossipServer(multiprocessing.Process):
    def __init__(self, addr, port, ):
        super(GossipServer, self).__init__()
        self.addr = addr
        self.port = port
