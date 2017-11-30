import logging
from configparser import RawConfigParser



def read_config(config_path):
    logging.debug("Parsing configuration from %s" % config_path)
    parser = RawConfigParser()
    parser.read(config_path)

    max_connections = parser.getint('CONN', 'max_connections')
    bootstrapper = parser.get('CONN', 'bootstrapper')
    listen_addr = parser.get('CONN', 'listen_addr')

    config = {'max_connections': max_connections, 'bootstrapper': bootstrapper, 'listen_addr': listen_addr}
    return config