import signal
import sys
import logging
import logging.config
import os
from argparse import ArgumentParser

from cassandra.conn.node import Node
from cassandra.engine.storage import DataStorage
from cassandra.gossip.gossiper import Gossiper
from cassandra.partitioner.ring_partitioner import RingPartitioner
from cassandra.server.server import CassandraServer
from configparser import RawConfigParser

DEFAULT_CONFIG_PATH = 'config/config.ini'

logging.config.fileConfig('config/logging_config.ini')


# noinspection PyShadowingNames
def signal_handler(signal, frame):
    logging.error('Stopping process with Pid(%s) signal(%s), frame(%s)' % (os.getpid(), signal, frame))
    sys.exit(0)


def main():
    parser = ArgumentParser()
    parser.add_argument('-c', '--config', help='Configuration file path', default=DEFAULT_CONFIG_PATH)
    args = parser.parse_args()

    config_parser = RawConfigParser()
    config_parser.read(args.config)

    logging.info('Starting main process - Pid: %s' % os.getpid())

    signal.signal(signal.SIGINT, signal_handler)

    node = Node(dict(config_parser['CONN']))

    gossiper = Gossiper(node)
    partitioner = RingPartitioner(node, dict(config_parser['PARTITIONER']))
    storager = DataStorage(node, dict(config_parser['STORAGER']))

    server = CassandraServer(node, partitioner, dict(config_parser['SERVER']))

    gossiper.start()
    partitioner.start()
    storager.start()
    server.start()

    node.start()

    gossiper.join()
    partitioner.join()
    storager.join()
