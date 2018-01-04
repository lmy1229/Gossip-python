from cassandra.client.client import CassandraClient

if __name__ == '__main__':
    client = CassandraClient()
    client.start()