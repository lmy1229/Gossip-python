import signal
import sys
import logging
import logging.config
import os
from argparse import ArgumentParser

from cassandra.conn.node import Node
from cassandra.util.message_codes import MESSAGE_CODE_NEW_CONNECTION, MESSAGE_CODE_GOSSIP, MESSAGE_CODE_CONNECTION_LOST
from cassandra.gossip.gossiper import Gossiper

DEFAULT_CONFIG_PATH = 'config/config.ini'

logging.config.fileConfig('config/logging_config.ini')


def signal_handler(signal, frame):
    logging.error('Stopping process - Pid: %s' % os.getpid())
    sys.exit(0)


def main():
    cli_parser = ArgumentParser()
    cli_parser.add_argument('-c', '--config', help='Configuration file path', default=DEFAULT_CONFIG_PATH)
    cli_args = cli_parser.parse_args()
    config_path = cli_args.config

    logging.info('Starting main process - Pid: %s' % os.getpid())

    signal.signal(signal.SIGINT, signal_handler)

    node = Node(config_path)

    gossiper = Gossiper(node)
    gossiper.start()
    node.start()
    gossiper.join()
