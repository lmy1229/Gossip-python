import logging
import mmh3
import random
from multiprocessing import Process

from cassandra.util.message_codes import *


class RingPartitioner(Process):
    def __init__(self, node, config):
        super(RingPartitioner, self).__init__()

        self.identifier = 'partitioner'
        node.register(self.identifier, MESSAGE_CODE_NEW_LIVE_NODE)
        node.register(self.identifier, MESSAGE_CODE_LOST_LIVE_NODE)
        self.message_manager = node.get_manager(self.identifier)

        self.source_addr = self.message_manager.get_self_addr()
        self.v_node_num = int(config.get('vnode', 3))
        self.replica_num = int(config.get('replica', 3))
        self.token2node = {}
        self.phy2node = {}
        self.node2token = {}
        self.dht = []
        self.u_bound = -(2 ** 31)
        self.l_bound = (2 ** 31) - 1
        self.partition_key = 0
        self.new_physical_node(str(self.source_addr[0]) + ':' + str(self.source_addr[1]))

    def run(self):
        handlers = {
            MESSAGE_CODE_NEW_LIVE_NODE: self.new_physical_node,
            MESSAGE_CODE_LOST_LIVE_NODE: self.delete_physical_node,
        }
        while True:
            msg = self.message_manager.get_msg()
            msg_body = msg['message'].get_values()
            logging.error('partitioner: %s' % msg)
            logging.error('partitioner: %s' % msg_body)
            phy_id = msg_body['source']
            handlers[msg['type']](phy_id)

    def set_partition_key(self, index):
        self.partition_key = index

    @staticmethod
    def get_random_token():
        """ To get a random token """
        upper_bound = (2 ** 31) - 1
        lower_bound = -(2 ** 31)
        seed = random.uniform(lower_bound, upper_bound)
        return mmh3.hash(seed)

    @staticmethod
    def get_token(input_key):
        """ To get the token to route the given key """
        return mmh3.hash(str(input_key))

    @staticmethod
    def get_node_token(v_id):
        return mmh3.hash(v_id)

    # add new nodes to the cluster
    def new_physical_node(self, phy_id):
        try:
            if phy_id in self.phy2node:
                raise KeyError('physical node already registered')
            else:
                self.phy2node[phy_id] = ([], 0)
                v_list = []
                for i in range(0, self.v_node_num):
                    v_id = str(phy_id) + '$' + str(i)
                    v_list.append(v_id)
                    self.new_node(v_id)
                self.phy2node[phy_id] = (v_list, self.v_node_num)
            logging.debug('partitioner: %s - %s - %s - %s - %s'
                          % (phy_id, self.dht, self.phy2node, self.node2token, self.token2node))
        except Exception as e:
            logging.error('partitioner error: %s (%s) error occurred - %s' % (self.dht, self.node2token, e), exc_info=True)

    def new_virtual_node(self, phy_id):
        try:
            if phy_id not in self.phy2node:
                raise KeyError('no such physical node')
            else:
                v_list, ver = self.phy2node[phy_id]
                v_id = str(phy_id) + '$' + str(ver)
                ver += 1
                self.new_node(v_id)
                v_list.append(v_id)
                self.phy2node[phy_id] = (v_list, ver)
        except Exception as e:
            logging.error('partitioner error: %s (%s) error occurred - %s' % (self.dht, self.node2token, e), exc_info=True)

    def new_node(self, v_id):
        token = self.get_node_token(v_id)
        self.token2node[token] = v_id
        self.node2token[v_id] = token
        self.token_insertion(token)

    def delete_physical_node(self, phy_id):
        if phy_id not in self.phy2node:
            return
        else:
            v_list, ver = self.phy2node.pop(phy_id)
            size = len(v_list)
            for i in range(0, size):
                v_id = v_list[i]
                self.delete_node(v_id)
        logging.debug('partitioner: %s - %s - %s - %s - %s'
                      % (phy_id, self.dht, self.phy2node, self.node2token, self.token2node))

    def delete_virtual_node(self, phy_id):
        try:
            if phy_id not in self.phy2node:
                raise KeyError('no such physical node')
            else:
                v_list, ver = self.phy2node[phy_id]
                if len(v_list) <= 1:
                    raise ValueError('no enough virtual node')
                v_id = v_list.pop()
                self.delete_node(v_id)
                self.phy2node[phy_id] = (v_list, ver)
        except Exception as e:
            logging.error('partitioner error: %s (%s) error occurred - %s' % (self.dht, self.node2token, e), exc_info=True)

    def delete_node(self, v_id):
        if v_id not in self.node2token:
            return
        else:
            token = self.node2token.pop(v_id)
            self.token2node.pop(token)
            self.token_deletion(token)

    def token_deletion(self, token):
        try:
            self.dht.remove(token)
            if len(self.dht) <= 0:
                self.u_bound = -(2 ** 63)
                self.l_bound = 2 ^ 31 - 1
            else:
                self.u_bound = self.dht[-1]
                self.l_bound = self.dht[0]

        except Exception as e:
            logging.error('partitioner error: %s (%s) error occurred %s' % (self.dht, self.node2token, e), exc_info=True)

    def token_insertion(self, new_token):
        size = len(self.dht)

        for i in range(0, size):
            if self.dht[i] > new_token:
                self.dht.insert(i, new_token)
                break
            else:
                continue
        else:
            self.dht.append(new_token)
            self.u_bound = new_token

        if self.l_bound > new_token:
            self.l_bound = new_token

    def find_replicas(self, key):
        try:
            row_token = self.get_token(key)
            size = len(self.dht)
            if size <= 0:
                raise KeyError('length error of dht: dht size is zero')

            pos = 0
            for i in range(0, size):
                if self.dht[i] > row_token:
                    pos = i
                    break
            dst_addrs = set()

            for j in range(0, self.replica_num):
                index = (pos + j) % size
                v_id = self.token2node[self.dht[index]]
                dst_addr = v_id.split('$')[0]
                dst_addrs.add(dst_addr)
            logging.error('find replica: dht %s \ node2token %s \ row_token %s \ dst_addr %s' % (self.dht, self.node2token, row_token, dst_addr))

            return list(dst_addrs)
        except Exception as e:
            logging.error('find replica error: %s (%s) error occurred - %s' % (self.dht, self.node2token, e), exc_info=True)

    def get_node_addrs(self, key):
        try:
            dst_addrs = self.find_replicas(key)
            logging.debug('partitioner: data key is %s, route to %s' % (key, dst_addrs))
            return list(dst_addrs)

        except Exception as e:
            logging.error('partitioner error: %s (%s) error occurred - %s' % (self.dht, self.node2token, e))
