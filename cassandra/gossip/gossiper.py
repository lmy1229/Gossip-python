import copy
import logging
import random
import time

from cassandra.gossip.GossipDigest import GossipDigest
from cassandra.gossip.GossipDigest import Serializable, GossipDigestSyn, GossipDigestAck, GossipDigestAck2
from cassandra.gossip.state import EndpointState, HeartBeatState
from cassandra.util.message import MESSAGE_TYPES
from cassandra.util.message_codes import *
from cassandra.util.packing import addr_tuple_to_str
from cassandra.util.scheduler import Scheduler


class Gossiper(Scheduler):

    def __init__(self, node, label='Gossiper'):
        self.identifier = label
        node.register(self.identifier, MESSAGE_CODE_NEW_CONNECTION)
        node.register(self.identifier, MESSAGE_CODE_CONNECTION_LOST)
        node.register(self.identifier, MESSAGE_CODE_GOSSIP)
        self.message_manager = node.get_manager(self.identifier)

        self.addr_str = '{addr[0]}:{addr[1]}'.format(addr=self.message_manager.get_self_addr())
        self.interval = 5
        self.liveEndpoints = set()  # address lists
        self.unreachableEndpoints = {}  # map from address to time stamp when lost
        self.endpointStateMap = {self.addr_str: EndpointState()}  # map from address to EndpointState
        # Timestamp to prevent processing any in-flight messages for we've not send any SYN yet
        self.firstSynSendAt = 0

        super(Gossiper, self).__init__(self.interval)

    def make_random_gossip_digest(self):
        endpoints = list(self.endpointStateMap.keys())
        g_digests = []
        random.shuffle(endpoints)
        for endpoint in endpoints:
            ep_state = self.endpointStateMap[endpoint]
            generation = ep_state.hbState.generation
            max_version = ep_state.get_max_endpoint_state_version()
            g_digests.append(GossipDigest(endpoint, generation, max_version))
        return g_digests

    def send_gossip(self, message, endpoints):
        size = len(endpoints)
        if size < 1:
            return False
        to = endpoints[random.randint(0, size-1)]

        if to == self.addr_str:
            logging.info("Ignore GossipDigestSyn to myself {} ...".format(to))
            return False

        logging.info("Sending a GossipDigestSyn to {} ...".format(to))

        if self.firstSynSendAt == 0:
            self.firstSynSendAt = time.time()
        self.message_manager.send_gossip_msg(to, message)
        return to in self.message_manager.seeds

    def maybe_gossip_to_unreachable_member(self, message):
        if len(self.unreachableEndpoints) > 0:
            if random.random() < float(len(self.unreachableEndpoints)) / (len(self.liveEndpoints) + 1):
                self.send_gossip(message, list(self.unreachableEndpoints.keys()))

    def maybe_gossip_to_seed(self, message):
        seeds = self.message_manager.get_seeds()
        if len(seeds) > 0:
            if len(seeds) == 1 and self.addr_str in seeds:
                return
            if len(self.liveEndpoints) == 0:
                self.send_gossip(message, seeds)
            else:
                prob = len(seeds) / float(len(self.liveEndpoints) + len(self.unreachableEndpoints))
                if random.random() <= prob:
                    self.send_gossip(message, seeds)

    def apply_state_locally(self, ep_state_map):
        for ep, remoteState in ep_state_map.items():
            if ep == self.addr_str:
                continue

            if ep in self.endpointStateMap:
                local_state = self.endpointStateMap[ep]
                local_gen = local_state.hbState.generation
                remote_gen = remoteState.hbState.generation
                local_time = time.time()

                logging.debug("{} local generation {}, remote generation {}".format(ep, local_gen, remote_gen))

                if remote_gen > local_time:
                    # assume some peer has corrupted memory and is broadcasting an unbelievable generation
                    # about another peer (or itself)
                    s = "Received an invalid gossip generation for peer {}; local time = {}, received generation = {}"
                    logging.warning(s.format(ep, local_time, remote_gen))

                elif remote_gen == local_gen:
                    # find maximum state
                    local_max_version = local_state.get_max_endpoint_state_version()
                    remote_max_version = remoteState.get_max_endpoint_state_version()

                    if remote_max_version > local_max_version:
                        # apply states, but do not notify since there is no major change
                        self.apply_new_state(ep, remoteState)

                    else:
                        pattern = "Ignoring remote version {} <= {} for {}"
                        logging.debug(pattern.format(remote_max_version, local_max_version, ep))

                else:
                    logging.debug("Ignoring remote generation {} < {}".format(remote_gen, local_gen))

            else:
                # this is a new node
                self.handle_major_state_change(ep, remoteState)

    def handle_major_state_change(self, ep, ep_state):
        """
        This method is called whenever there is a "big" change in ep state (a generation change for a known node).
        :param ep:      endpoint
        :param ep_state: EndpointState for the endpoint
        """
        logging.debug("Adding endpoint state for {}".format(ep))
        self.endpointStateMap[ep] = ep_state
        self.liveEndpoints.add(ep)
        self.unreachableEndpoints.pop(ep, None)
        self.send_alive_notification(ep)

    def send_alive_notification(self, ep):
        msg = MESSAGE_TYPES[MESSAGE_CODE_NEW_LIVE_NODE](bytes(ep, 'ascii'))
        self.message_manager.send_notification(msg)
        logging.debug("send alive node %s ..." % msg)

    def apply_new_state(self, address, remote_state):
        old_version = self.endpointStateMap[address].hbState.version
        local = self.endpointStateMap[address]
        local.hbState = copy.deepcopy(remote_state.hbState)
        logging.debug("Updating heartbeat state version to {} from {} for {} ...".format(
                         self.endpointStateMap[address].hbState.version, old_version, address))

        assert remote_state.hbState.generation == local.hbState.generation
        local.add_application_states(remote_state.applicationStates)

        return self.endpointStateMap[address]

    def examine_gossiper(self, g_digests):
        delta_gossip_digest_list = set([])
        delta_ep_state_map = {}

        # Here we need to fire a GossipDigestAckMessage. If we have some data associated with this endpoint locally
        # then we follow the "if" path of the logic. If we have absolutely nothing for this endpoint we need to
        # request all the data for this endpoint.
        for gDigest in g_digests:

            if gDigest.endpoint in self.endpointStateMap:
                ep_state = self.endpointStateMap[gDigest.endpoint]
                max_local_version = ep_state.get_max_endpoint_state_version()

                if gDigest.generation == ep_state.hbState.generation and max_local_version == gDigest.maxVersion:
                    continue

                if gDigest.generation > ep_state.hbState.generation:
                    # request everything from the gossiper
                    delta_gossip_digest_list.add(self.request_all(gDigest))

                elif gDigest.generation > ep_state.hbState.generation:
                    delta_ep_state_map = {**delta_ep_state_map, **self.send_all(gDigest, 0)}

                else:
                    # If the max remote version is lesser, then we send all the data we have locally for this endpoint
                    # with version greater than the max remote version.
                    if max_local_version > gDigest.maxVersion:
                        delta_ep_state_map = {**delta_ep_state_map, **self.send_all(gDigest, gDigest.maxVersion)}

                    # If the max remote version is greater then we request the remote endpoint send us all the data
                    # for this endpoint with version greater than the max version number we have locally for this
                    # endpoint.
                    elif max_local_version < gDigest.maxVersion:
                        gossip_digest = GossipDigest(gDigest.endpoint, gDigest.maxVersion, max_local_version)
                        delta_gossip_digest_list.add(gossip_digest)

            else:
                # We are here since we have no data for this endpoint locally so request everything.
                delta_gossip_digest_list.add(self.request_all(gDigest))

        return delta_gossip_digest_list, delta_ep_state_map

    @staticmethod
    def request_all(g_digest):
        """
        Request all the state for the endpoint in the g_digest
        """
        logging.debug("request_all for {}".format(g_digest.endpoint))
        return GossipDigest(g_digest.endpoint, g_digest.generation, 0)

    def send_all(self, g_digest, max_remote_version):
        """
        Send all the data with version greater than max_remote_version
        """
        local = self.get_state_for_version_bigger_than(g_digest.endpoint, max_remote_version)
        if local is not None:
            return {g_digest.endpoint: local}

    def get_state_for_version_bigger_than(self, endpoint, version):
        if endpoint in self.endpointStateMap:
            ep_state = self.endpointStateMap[endpoint]
            ret, states = None, {}
            hb = None

            if ep_state.hbState.version > version:
                hb = HeartBeatState(ep_state.hbState.generation, ep_state.hbState.version)

            for key, versioned_value in ep_state.applicationStates.items():
                if versioned_value.version > version:
                    if ret is None:
                        hb = HeartBeatState(ep_state.hbState.generation, ep_state.hbState.version)

                    states[key] = versioned_value
                    logging.debug("Adding state {}: {}".format(key, versioned_value.value))

            if hb is not None:
                return EndpointState(hb, states)

    def interval_task(self):
        """
        this function will run every second
        """
        try:
            hb_state = self.endpointStateMap[self.addr_str].hbState
            hb_state.update_heart_beat()
            # TODO update other information of mine.
            logging.debug("My heartbeat is now {}".format(hb_state.version))
            g_digests = self.make_random_gossip_digest()
            if len(g_digests) > 0:
                message = GossipDigestSyn(g_digests).serialize()
                gossiped_to_seed = self.send_gossip(message, list(self.liveEndpoints))
                self.maybe_gossip_to_unreachable_member(message)
                if not gossiped_to_seed or len(self.liveEndpoints) < len(self.message_manager.get_seeds()):
                    self.maybe_gossip_to_seed(message)
                    # self.doStatusCheck()

            else:
                logging.debug('Not sending GossipSyn because g_digests is empty')
        except Exception as e:
            logging.error("Gossip error: {}".format(e))
            exit(1)

    def main_task(self):
        handlers = {
            MESSAGE_CODE_NEW_CONNECTION: self.new_connection_handler,
            MESSAGE_CODE_CONNECTION_LOST: self.connection_lost_handler,
            MESSAGE_CODE_GOSSIP: self.gossip_handler,

        }
        while True:
            msg = self.message_manager.get_msg()
            handlers[msg['type']](msg['message'], addr_tuple_to_str(msg['message'].source_addr))

    def new_connection_handler(self, msg, remote_identifier):
        logging.debug("new connection from {}, add it to liveEndpoints list. msg: %s".format(remote_identifier, msg))
        self.liveEndpoints.add(remote_identifier)
        # logging.debug("Current liveEndPoints is {}".format(str(self.liveEndpoints)))

    def connection_lost_handler(self, msg, remote_identifier):
        logging.debug("connection lost from %s, remove from liveEndpoints to unreachableEndpoints" % remote_identifier)
        logging.debug("msg %s" % msg)

        logging.error('-'*60)
        logging.error(self.liveEndpoints)
        logging.error('-'*60)

        self.liveEndpoints.remove(remote_identifier)
        self.unreachableEndpoints[remote_identifier] = time.time()
        s_addr = (msg.get_values())['source']
        s_addr = str(s_addr[0]) + ':' + str(s_addr[1])
        self.send_lost_notification(s_addr)
        # logging.debug("current liveEndPoints is {}".format(str(self.liveEndpoints)))
        # logging.debug("current unreachableEndpoints is {}".format(str(self.unreachableEndpoints)))

    def send_lost_notification(self, ep):
        msg = MESSAGE_TYPES[MESSAGE_CODE_LOST_LIVE_NODE](bytes(ep, 'ascii'))
        self.message_manager.send_notification(msg)
        logging.debug("send lost node %s ..." % msg)

    def gossip_handler(self, msg, remote_identifier):
        if self.firstSynSendAt == 0:
            logging.debug("ignore message because we have not send any syn message yes.")
            return
        msg_cls = Serializable.deserialize(msg.data)

        logging.debug('Received an gossip message {} from {}'.format(msg_cls, remote_identifier))

        if isinstance(msg_cls, GossipDigestSyn):
            delta_gossip_digest_list, delta_ep_state_map = self.examine_gossiper(msg_cls.gDigests)
            gossip_digest_ack = GossipDigestAck(delta_gossip_digest_list, delta_ep_state_map)
            logging.debug('sending gossip digest ack {} to {}'.format(gossip_digest_ack, remote_identifier))
            self.message_manager.send_gossip_msg(remote_identifier, gossip_digest_ack.serialize())

        elif isinstance(msg_cls, GossipDigestAck):
            if len(msg_cls.epStateMap) > 0:
                if time.time() < self.firstSynSendAt or self.firstSynSendAt == 0:
                    logging.debug("Ignoring unrequested GossipDigestAck from {}".format(remote_identifier))
                self.apply_state_locally(msg_cls.epStateMap)

            # Get the state required to send to this gossip - construct GossipDigestAck2Message
            delta_ep_state_map = {}
            for gDigest in msg_cls.gDigests:
                state = self.get_state_for_version_bigger_than(gDigest.endpoint, gDigest.maxVersion)

                if state is not None:
                    delta_ep_state_map[gDigest.endpoint] = state

            if len(delta_ep_state_map) > 0:
                logging.debug('Sending a GossipDigestAck2Message to {}'.format(remote_identifier))
                gossip_digest_ack2 = GossipDigestAck2(delta_ep_state_map)
                self.message_manager.send_gossip_msg(remote_identifier, gossip_digest_ack2.serialize())

            else:
                logging.debug('No state is newer, not send GossipDigestAck2Message to {}'.format(remote_identifier))

        elif isinstance(msg_cls, GossipDigestAck2):
            self.apply_state_locally(msg_cls.epStateMap)
