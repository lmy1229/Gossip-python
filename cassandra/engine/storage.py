import csv
import json
import logging
import os
import time
import signal
import sys
from multiprocessing import Process

from cassandra.util.cache import LRUCache
from cassandra.util.message import ResponseMessage
from cassandra.util.message_codes import MESSAGE_CODE_REQUEST
from cassandra.util.packing import addr_tuple_to_str


class DataStorage(Process):
    """Data storage for naive cassandra

    Index file format: CSV (delimited by comma(,))
        key1,start,length
        key2,start,length
        ...

    Data file format: ASCII (sorted on key, keys not in this file)
    """

    DATA_FILE_EXT = '.ssdf'
    INDEX_FILE_EXT = '.ssif'

    def __init__(self, node, config):
        super(DataStorage, self).__init__()

        self.label = "DataStorage"
        node.register(self.label, MESSAGE_CODE_REQUEST)
        self.manager = node.get_manager(self.label)

        # configurations
        self.datafile_dir = config.get('datafile_dir', 'data/')
        self.max_indices_in_memory = int(config.get('max_indices_in_memory', -1))
        self.max_data_per_sstable = int(config.get('max_data_per_sstable', 2 ** 20))  # 1M

        self.table_indices = LRUCache(self.max_indices_in_memory)
        self.table_index_names = []
        self.memtable = {}
        self.memversions = {}
        self.memtable_size = 0

        self.load_dir(self.datafile_dir)

    def put(self, key, data):

        try:
            if key in self.memtable:
                self.memtable[key] = data
                self.memversions[key] = self.memversions[key] + 1
                return True, 'Ok'

            length = len(data)
            if self.memtable_size + length > self.max_data_per_sstable:
                self.flush_to_file()
            self.memtable[key] = data
            self.memtable_size = self.memtable_size + length
            self.memversions[key] = self.get_version(key) + 1
            return True, 'Ok'

        except Exception as e:
            error_message = 'Error occurred when put (%s, %s) into database: %s' % (key, data, e)
            logging.error('%s | %s' % (self.label, error_message), exc_info=True)
            return False, error_message

    def get(self, key):
        try:
            # return True, self.get_data_from_memtable(key) + self.get_data_from_sstables(key)
            data = self.get_data_from_memtable(key)
            if len(data) == 0:
                data = self.get_data_from_sstables(key)
            return True, data

        except Exception as e:
            error_message = 'Error occurred when get (%s) into database: %s' % (key, e)
            logging.error('%s | %s' % (self.label, error_message), exc_info=True)
            return False, error_message

    def update(self, key, value, version):
        try:
            self.memtable[key] = value
            self.memversions[key] = version
            return True, 'update version of %s to %d' % (key, version)
        except Exception as e:
            error_message = 'Error occurred when get (%s) into database: %s' % (key, e)
            logging.error('%s | %s' % (self.label, error_message), exc_info=True)
            return False, error_message

    def get_index_file_path(self, index_file_name):
        return os.path.join(self.datafile_dir, index_file_name + DataStorage.INDEX_FILE_EXT)

    def get_data_file_path(self, data_file_name):
        return os.path.join(self.datafile_dir, data_file_name + DataStorage.DATA_FILE_EXT)

    def load_dir(self, datafile_dir):
        # read the datafile_dir and initiate the table_index_names
        if not os.path.exists(datafile_dir):
            os.makedirs(datafile_dir)

        data_file_list = []
        index_file_list = []

        for f in os.listdir(datafile_dir):
            if os.path.isfile(os.path.join(datafile_dir, f)):
                name, ext = os.path.splitext(f)
                if ext == DataStorage.DATA_FILE_EXT:
                    data_file_list.append(name)
                elif ext == DataStorage.INDEX_FILE_EXT:
                    index_file_list.append(name)
        self.table_index_names = [i for i in data_file_list if i in index_file_list]

        diff1 = [i for i in data_file_list if i not in index_file_list]
        diff2 = [i for i in index_file_list if i not in data_file_list]

        for name in diff1:
            logging.error('DataStorage | IndexFile for %s not found. Ignoring.' % name)
        for name in diff2:
            logging.error('DataStorage | Datafile for %s not found. Ignoring.' % name)

    def flush_to_file(self):
        if len(self.memtable) == 0:
            return
        index_key = str(int(time.time() * 1000))
        with open(self.get_index_file_path(index_key), 'w') as index_file:
            with open(self.get_data_file_path(index_key), 'wb') as data_file:
                keys = sorted(self.memtable.keys())
                offset = 0
                for key in keys:
                    data = self.memtable[key]
                    length = len(data)
                    version = self.memversions[key]
                    index_file.write(','.join([key, str(offset), str(length), str(version)]) + '\n')
                    data_file.write(bytes(data, 'ascii'))
                    offset = offset + length
                # padding to fix number
                # padding = self.max_data_per_sstable - offset
                # data_file.write(bytes([0] * padding))
                # clear memtable
                self.memtable.clear()
                self.memtable_size = 0
                self.table_index_names.append(index_key)

    def read_index_file(self, index_key):
        index_file_path = self.get_index_file_path(index_key)
        with open(index_file_path, 'r') as csv_file:
            index = {}
            csv_data = csv.reader(csv_file, delimiter=',')
            for row in csv_data:
                index[row[0]] = [int(row[1]), int(row[2]), int(row[3])]
            self.table_indices.set(index_key, index)
            return index

    def get_data_from_memtable(self, key):
        d = self.memtable.get(key, None)
        if d:
            return [d, self.memversions[key]]
        else:
            return []

    def get_data_from_sstables(self, key):
        # TODO: find another policy
        for index_key in sorted(self.table_index_names, reverse=True):
            d = self.get_data_from_sstable(key, index_key)
            if d:
                return d
        return []

    def get_data_from_sstable(self, key, index_key):
        ol = self.search_in_index(key, index_key)
        if ol:
            offset, length, version = ol
            data_file_path = self.get_data_file_path(index_key)
            with open(data_file_path, 'rb') as datafile:
                datafile.seek(offset, 0)
                data = datafile.read(length)
                return [data.decode(), version]
        else:
            return None

    def get_version(self, key):
        if key in self.memversions:
            return self.memversions[key]
        d = self.search_in_indices(key)
        if d:
            return d[2]
        else:
            return 0

    def search_in_indices(self, key):
        for index_key in sorted(self.table_index_names, reverse=True):
            d = self.search_in_index(key, index_key)
            if d:
                return d
        return

    def search_in_index(self, key, index_key):
        index = self.table_indices.get(index_key)
        if not index:
            index = self.read_index_file(index_key)
        return index.get(key, None)

    def singal_handler(self, signal, frame):
        self.flush_to_file()
        logging.error('Storage: Stopping process with Pid(%s) signal(%s), frame(%s)' % (os.getpid(), signal, frame))
        sys.exit(0)

    def run(self):
        logging.info('%s started - Pid: %ds' % (self.label, self.pid))
        signal.signal(signal.SIGINT, self.singal_handler)
        while True:
            msg = self.manager.get_msg()

            remote_identifier = addr_tuple_to_str(msg['message'].source_addr)

            values = msg['message'].get_values()

            if values['code'] == MESSAGE_CODE_REQUEST:
                request = values['request']
                request_hash = values['request_hash']

                if request[0] == 'put':
                    status, description = self.put(request[1], request[2])
                elif request[0] == 'get':
                    status, description = self.get(request[1])
                elif request[0] == 'update':
                    status, description = self.update(request[1], request[2], request[3])
                else:
                    description = 'Request can not be recognized: %s' % request
                    status = False
                    logging.error('%s | %s' % (self.label, description), exc_info=True)

                d = {'status': status, 'description': description, 'request_hash': request_hash}
                s = bytes(json.dumps(d), 'ascii')
                msg_to_send = ResponseMessage(s, self.manager.get_self_addr())
                self.manager.send_msg_object(remote_identifier, msg_to_send)
            else:
                logging.error('%s | Unsupported message type %s in message %s'
                              % (self.label, values['code'], str(values)))
