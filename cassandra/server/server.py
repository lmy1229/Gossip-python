from cassandra.util.scheduler import Scheduler
from cassandra.conn.server import Server
from cassandra.conn.sender import Sender
from multiprocessing import Process, Queue
from cassandra.partitioner.ring_partitioner import RingPartitioner
from cassandra.util.message import GetRequestMessage, PutRequestMessage
from cassandra.util.packing import successed_message, failed_message
from cassandra.server.controller import CassandraController
import logging
import socket
import json


class CassandraServer(Process):
    """Server for Cassandra"""
    def __init__(self, addr, port, node, max_conn=5, label='CassandraServer'):
        super(CassandraServer, self).__init__()



        self.addr = addr
        self.port = port
        self.label = label
        self.manager = manager
        self.partitioner = partitioner

        self.sender_queue = Queue()
        self.receiver_queue = Queue()

        connection_pool = ConnectionPool(self.label+"(Pool)", self.max_conn)

        self.server = Server(
            label=self.label+"(Server)",
            receiver_label=self.label+"(Receiver)",
            addr=self.addr,
            port=self.port,
            to_queue=self.receiver_queue,
            connection_pool=connection_pool,
            listen_addr=(self.addr, self.port),
            max_conn=max_conn)

        self.sender = Sender(
            label=self.label+"(Sender)",
            reciever_label=self.label+"(Receiver)",
            from_queue=self.sender_queue,
            to_queue=self.receiver_queue,
            connection_pool=connection_pool,
            listen_addr=(self.addr, self.port))

        self.controller = CassandraController(
            label=self.label+"(Controller)",
            from_queue=self.receiver_queue,
            to_queue=self.sender_queue,
            manager=self.manager,
            partitioner=self.partitioner
        )

    def run(self):
        self.server.start()
        self.sender.start()
        self.controller.start()
