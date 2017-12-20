import signal
import sys 
import logging
import logging.config
import os
from argparse import ArgumentParser
import time
import multiprocessing

from cassandra.conn.node import Node
from cassandra.util.message_codes import MESSAGE_CODE_GOSSIP, MESSAGE_CODE_NEW_CONNECTION

DEFAULT_CONFIG_PATH = 'config/config.ini'

logging.config.fileConfig('config/logging_config.ini')

def signal_handler(signal, frame):
    logging.error('Stopping process - Pid: %s' % os.getpid())
    sys.exit(0)


def main(config_path):

    logging.info('Starting main process - Pid: %s' % os.getpid())

    signal.signal(signal.SIGINT, signal_handler)

    node = Node(config_path)

    node.start()
