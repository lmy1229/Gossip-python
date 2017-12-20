from cassandra.util.scheduler import Scheduler
from cassandra.conn.server import Server
from multiprocessing import Process, Queue
import logging
import socket

class CassandraServer(Process):
    """Server for Cassandra"""
    def __init__(self, label, addr, port, manager):
        super(CassandraServer, self).__init__()
        self.addr = addr
        self.port = port
        self.label = label
        self.manager = manager

        self.sender_queue = Queue()
        self.receiver_queue = Queue()
        self.pool = 
        
        self.server = Server(self.label+"(Server)", )
        self.sender = 

    def run(self):
        self.server.start()
        self.
