import json
import os
import logging
import csv
import time
from multiprocessing import Process
from cassandra.util.message_codes import MESSAGE_CODE_PUT_REQUEST, MESSAGE_CODE_GET_REQUEST, MESSAGE_CODE_RESPONSE
from cassandra.util.packing import successed_message, failed_message

from cassandra.util.cache import LRUCache


<<<<<<< Updated upstream
class DataStorage(Process):
=======
class DataStorage():
>>>>>>> Stashed changes
    """Data storage for naive cassandra

    Index file format: CSV (delimitted by comma(,))
        key1,start,length
        key2,start,length
        ...

    Data file format: ASCII (sorted on key, keys not in this file)
    """

    DATA_FILE_EXT = '.ssdf'
    INDEX_FILE_EXT = '.ssif'

    def __init__(self, node, **kwargs):
        super(DataStorage, self).__init__()

        self.label = "DataStorage"
        node.register(self.label, MESSAGE_CODE_PUT_REQUEST)
        node.register(self.label, MESSAGE_CODE_GET_REQUEST)
        self.manager = node.get_manager(self.identifier)

        # configurations
        self.datafile_dir = kwargs.get('datafile_dir', 'data/')
        self.max_indices_in_memory = kwargs.get('max_indices_in_memory', -1)
        self.max_data_per_sstable = kwargs.get('max_data_per_sstable', 2 ** 20)  # 1M

        self.table_indices = LRUCache(self.max_indices_in_memory)
        self.table_index_names = []
        self.memtable = {}
        self.memtable_size = 0

        self.load_dir(self.datafile_dir)

    def put(self, key, data):

        if key in self.memtable:
            self.memtable[key] = data
            return

        length = len(data)
        if self.memtable_size + length > self.max_data_per_sstable:
            self.flush_to_file()
        self.memtable[key] = data
        self.memtable_size = self.memtable_size + length
        return

    def get(self, key):
        return self.get_data_from_memtable(key) + self.get_data_from_sstables(key)

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
            logging.error('DataStorage | Indexfile for %s not found. Ignoring.' % name)
        for name in diff2:
            logging.error('DataStorage | Datafile for %s not found. Ignoring.' % name)

    def flush_to_file(self):
        index_key = str(int(time.time() * 1000))
        with open(self.get_index_file_path(index_key), 'w') as ifile:
            with open(self.get_data_file_path(index_key), 'wb') as dfile:
                keys = sorted(self.memtable.keys())
                offset = 0
                for key in keys:
                    data = self.memtable[key]
                    length = len(data)
                    ifile.write(','.join([key, str(offset), str(length)]) + '\n')
                    dfile.write(bytes(data, 'ascii'))
                    offset = offset + length
                # padding to fix number
                padding = self.max_data_per_sstable - offset
                dfile.write(bytes([0] * padding))
                # clear memtable
                self.memtable.clear()
                self.memtable_size = 0
                self.table_index_names.append(index_key)

    def read_index_file(self, index_key):
        index_file_path = self.get_index_file_path(index_key)
        with open(index_file_path, 'r') as csvfile:
            index = {}
            csvdata = csv.reader(csvfile, delimiter=',')
            for row in csvdata:
                index[row[0]] = [int(row[1]), int(row[2])]
            self.table_indices.set(index_key, index)
            return index

    def get_data_from_memtable(self, key):
        d = self.memtable.get(key, None)
        if d:
            return [d]
        else:
            return []

    def get_data_from_sstables(self, key):
        # TODO: find another policy
        data = []
        for index_key in self.table_index_names:
            d = self.get_data_from_sstable(key, index_key)
            if d:
                data.append(d)
        return data

    def get_data_from_sstable(self, key, index_key):
        ol = self.search_in_index(key, index_key)
        if ol:
            offset, length = ol
            data_file_path = self.get_data_file_path(index_key)
            with open(data_file_path, 'rb') as datafile:
                datafile.seek(offset, 0)
                data = datafile.read(length)
                return data.decode()
        else:
            return None

    def search_in_index(self, key, index_key):
        index = self.table_indices.get(index_key)
        if not index:
            index = self.read_index_file(index_key)
        return index.get(key, None)

    def run(self):
        logging.info('%s started - Pid: %ds' % (self.label, self.pid))

        while True:
            msg = self.manager.get_msg()

            # TODO, clean this
            remote_identifier = msg['message'].source_addr
            if remote_identifier is None:
                remote_identifier = msg['remote_identifier']
            elif not isinstance(remote_identifier, str):
                remote_identifier = "{0[0]}:{0[1]}".format(remote_identifier)

            values = msg['message'].get_values()
            if msg['type'] == MESSAGE_CODE_PUT_REQUEST:
                self.put(values['key'], values['value'])
                resp = successed_message()
            elif msg['type'] == MESSAGE_CODE_GET_REQUEST:
                resp = successed_message(self.get(values['key']))
            else:
                resp = failed_message('Unsupported message type %s in message %s' % (msg['type'], str(msg)))

            s = bytes(json.dumps(resp), 'ascii')
            self.manager.send_msg_object(remote_identifier, StorageResponseMessage(s))
