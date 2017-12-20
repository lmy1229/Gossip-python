from argparse import ArgumentParser
from test import test_gossip_receive, test_gossip_send, test_gossip_connection, test_gossip_notification, test_conn_node

DEFAULT_CONFIG_PATH = "config/config.ini"
DEFAULT_TEST = "send"

if __name__ == '__main__':
    cli_parser = ArgumentParser()
    cli_parser.add_argument('-c', '--config', help='Configuration file path', default=DEFAULT_CONFIG_PATH)
    cli_parser.add_argument('-t', '--test', help='test to run', default=DEFAULT_TEST)
    cli_args = cli_parser.parse_args()

    config_path = cli_args.config
    test_name = cli_args.test

    if test_name == 'send':
        test_gossip_send.main(config_path)
    elif test_name == 'receive':
        test_gossip_receive.main(config_path)
    elif test_name == 'connection':
        test_gossip_connection.main(config_path )
    elif test_name == 'notification':
        test_gossip_notification.main(config_path)
    elif test_name == 'node':
        test_conn_node.main(config_path)