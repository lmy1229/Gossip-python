import signal
import sys 
import logging
import logging.config
import os
from argparse import ArgumentParser
import time
import multiprocessing

from gossip.conn.node import Node
from gossip.util.message_codes import MESSAGE_CODE_GOSSIP, MESSAGE_CODE_NEW_CONNECTION

DEFAULT_CONFIG_PATH = 'config/config.ini'

logging.config.fileConfig('config/logging_config.ini')

def signal_handler(signal, frame):
    logging.error('Stopping process - Pid: %s' % os.getpid())
    sys.exit(0)

def receive(manager):
    while True:
        msg = manager.get_msg()
        if msg:
            if msg['type'] == MESSAGE_CODE_GOSSIP:
                print('APP: get msg %s' % msg['message'].data)
            elif msg['type'] == MESSAGE_CODE_NEW_CONNECTION:
                print('APP: get new connection from %s' % msg['message'].remote_identifier)
        time.sleep(0.5)

def main(config_path):

    logging.info('Starting main process - Pid: %s' % os.getpid())

    signal.signal(signal.SIGINT, signal_handler)

    node = Node(config_path)

    node.register('test_sender', MESSAGE_CODE_GOSSIP)
    node.register('test_sender', MESSAGE_CODE_NEW_CONNECTION)

    manager = node.get_manager('test_sender')

    p = multiprocessing.Process(target=receive, args = (manager, ))
    p.start()

    node.start()
