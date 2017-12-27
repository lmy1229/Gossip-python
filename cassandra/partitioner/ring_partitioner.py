import mmh3
import random
import logging
import json
from cassandra.util.config_parser import read_config
from multiprocessing import Process
from cassandra.util.message import *
from cassandra.util.message_codes import *


class RingPartitioner(Process):

    def __init__(self, message_manager, config_path):
        super(RingPartitioner, self).__init__()
        self.message_manager = message_manager
        self.config_path = config_path
        self.config = read_config(config_path)
        self.source_addr = self.message_manager.get_self_addr()
        self.v_node_num = self.config.get('vnode', 3)
        self.replica_num = self.config.get('replica', 3)
        self.token2node = {}
        self.phy2node = {}
        self.node2token = {}
        self.dht = []
        self.u_bound = -(2 ** 31)
        self.l_bound = (2 ** 31) - 1
<<<<<<< Updated upstream
        self.partition_key = 0
        self.new_physical_node(str(self.source_addr[0]) + ':' + str(self.source_addr[1]))
=======
        raw_id = self.message_manager.get_self_addr()
        self.new_physical_node(str(raw_id[0]) + ':' + str(raw_id[1]))
>>>>>>> Stashed changes

    def run(self):
        handlers = {
            MESSAGE_CODE_NEW_LIVE_NODE: self.new_physical_node,
            MESSAGE_CODE_LOST_LIVE_NODE: self.delete_physical_node,
        }
        while True:
            msg = self.message_manager.get_msg()
            msg_body = msg['message'].get_values()
            logging.debug('partitioner: %s' % (msg))
            logging.debug('partitioner: %s' % (msg_body))
            phy_id = msg_body['source']
            handlers[msg['type']](phy_id)

<<<<<<< Updated upstream
    def set_partition_key(self, index):
        self.partition_key = index

=======
>>>>>>> Stashed changes
    # to get a random token
    def get_random_token(self):
        ubound = (2 ** 31) - 1
        lbound = -(2 ** 31)
        seed = random.uniform(lbound, ubound)
        return (mmh3.hash(seed))

    # to get the token to route the given key
    def get_token(self, input_key):
        return (mmh3.hash(str(input)))

    def get_node_token(self, v_id):
        return (mmh3.hash(v_id))

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
<<<<<<< Updated upstream
                self.phy2node[phy_id] = (v_list, self.v_node_num)
            logging.debug('partitioner: %s - %s - %s - %s - %s'
                          % (phy_id, self.dht, self.phy2node, self.node2token, self.token2node))
            self.data_route([1, 2])
=======
                self.phy2node[phy_id] = (v_list, 3)
            logging.debug('partitioner: %s - %s - %s - %s - %s'
                          % (phy_id, self.dht, self.phy2node, self.node2token, self.token2node))
            self.data_route(1)  # TODO
>>>>>>> Stashed changes
        except Exception as e:
            logging.error('partitioner error: %s (%s) error occured - %s' % (self.dht, self.node2token, e))

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
            logging.error('partitioner error: %s (%s) error occured - %s' % (self.dht, self.node2token_map, e))

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
        logging.debug('partitioner:%s - %s - %s - %s - %s'
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
            logging.error('partitioner error: %s (%s) error occured - %s' % (self.dht, self.node2token_map, e))

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
            logging.error('partitioner error: %s (%s) error occured - %s' % (self.dht, self.node2token_map, e))

    def token_insertion(self, new_token):
        size = len(self.dht)
        l_flag = False

        for i in range(0, size):
            if self.dht[i] > new_token:
                l_flag = True
                break

        if not l_flag:
            self.dht.append(new_token)
            self.u_bound = new_token
        else:
            self.dht.insert(i, new_token)

        if self.l_bound > new_token:
            self.l_bound = new_token

<<<<<<< Updated upstream
    def find_replicas(self, key):
        try:
            row_token = self.get_token(key)
            size = len(self.dht)
            if size <= 0:
                raise KeyError('length error of dht: dht size is zero')
            l_flag = False
            for i in range(0, size):
                if self.dht[i] > row_token:
                    l_flag = True
                    break
            if not l_flag:
                i = 0
            dst_addrs = set()
            for j in range(0, self.replica_num):
                index = (i + j) % size
                v_id = self.token2node[self.dht[index]]
                dst_addr = v_id.split('$')[0]
                dst_addrs.append(dst_addr)
            return list(dst_addrs)
        except Exception as e:
            logging.error('find replica error: %s (%s) error occured - %s' % (self.dht, self.node2token, e))
=======
    def data_route(self, key):
        row_token = self.get_token(key)
        size = len(self.dht)
        l_flag = False
        for i in range(0, size):
            if self.dht[i] > row_token:
                l_flag = True
                break
>>>>>>> Stashed changes

    def get_node_addrs(self, key):
        try:
            dst_addrs = self.find_replicas(key)
            logging.debug('partitioner: data key is %s, route to %s' % (row_token, dst_addrs))
            return list(dst_addrs)

<<<<<<< Updated upstream
        except Exception as e:
            logging.error('partitioner error: %s (%s) error occured - %s' % (self.dht, self.node2token, e))
=======
        v_id = self.token2node[node_token]
        logging.debug('partitioner: data key is %s, route to %s' % (row_token, v_id))

        return v_id
>>>>>>> Stashed changes
