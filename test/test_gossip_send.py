import signal
import sys 
import logging
import logging.config
import os
from argparse import ArgumentParser
import time
import multiprocessing

from cassandra.conn.node import Node
from cassandra.util.message_codes import MESSAGE_CODE_GOSSIP

DEFAULT_CONFIG_PATH = 'config/config.ini'

logging.config.fileConfig('config/logging_config.ini')

def signal_handler(signal, frame):
    logging.error('Stopping process - Pid: %s' % os.getpid())
    sys.exit(0)

def send(manager):
    hb = 0
    while True:
        time.sleep(1)
        manager.send_gossip_msg('127.0.0.1:7001', bytes([hb]))
        hb = (hb + 1) % 256

def main(config_path):

    logging.info('Starting main process - Pid: %s' % os.getpid())

    signal.signal(signal.SIGINT, signal_handler)

    node = Node(config_path)

    node.register('test_sender', MESSAGE_CODE_GOSSIP)

    manager = node.get_manager('test_sender')

    p = multiprocessing.Process(target=send, args = (manager, ))
    p.start()

    node.start()
