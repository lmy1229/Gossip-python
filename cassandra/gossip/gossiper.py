import time
import random
import logging
from cassandra.gossip.state import EndpointState, HeartBeatState
from cassandra.gossip.GossipDigest import GossipDigest, GossipDigestSyn
from cassandra.util.message_codes import MESSAGE_CODE_NEW_CONNECTION
from cassandra.util.message_codes import MESSAGE_CODE_CONNECTION_LOST
from cassandra.util.message_codes import MESSAGE_CODE_GOSSIP
from cassandra.util.message import GossipMessage
from cassandra.gossip.GossipDigest import Serializable, GossipDigestSyn, GossipDigestAck, GossipDigestAck2
from cassandra.util.scheduler import Scheduler
from pprint import pprint
import copy


class Gossiper(Scheduler):

    def __init__(self, message_manager):
        self.message_manager = message_manager
        # Maximimum difference between generation value and local time we are willing to accept about a peer
        self.interval = 5  # TODO
        self.liveEndpoints = set()  # address lists
        self.unreachableEndpoints = {}  # map from address to time stamp when lost
        self.endpointStateMap = {self.message_manager.get_self_addr(): EndpointState()}  # map from address to EndpointState
        # Timestamp to prevent processing any in-flight messages for we've not send any SYN yet
        self.firstSynSendAt = 0

        super(Gossiper, self).__init__(self.interval)

    def makeRandomGossipDigest(self):
        endpoints = list(self.endpointStateMap.keys())
        gDigests = []
        random.shuffle(endpoints)
        for endpoint in endpoints:
            epState = self.endpointStateMap[endpoint]
            generation = epState.hbState.generation
            maxVersion = epState.getMaxEndpointStateVersion()
            gDigests.append(GossipDigest(endpoint, generation, maxVersion))
        return gDigests

    def sendGossip(self, message, endpoints):
        size = len(endpoints)
        if size < 1:
            return False
        to = endpoints[random.randint(0, size-1)]

        logging.debug("Sending a GossipDigestSyn to {} ...".format(to))

        if self.firstSynSendAt == 0:
            self.firstSynSendAt = time.time()
        self.message_manager.send_gossip_msg(to, message)
        return to in self.message_manager.seeds

    def doGossipToLiveMember(self, message):
        return self.sendGossip(message, list(self.liveEndpoints))

    def maybeGossipToUnreachableMember(self, message):
        if len(self.unreachableEndpoints) > 0:
            if random.random() < float(len(self.unreachableEndpoints)) / (len(self.liveEndpoints) + 1):
                self.sendGossip(message, list(self.unreachableEndpoints.keys()))

    def maybeGossipToSeed(self, message):
        seeds = self.message_manager.get_seeds()
        if len(seeds) > 0:
            if len(seeds) == 1 and self.message_manager.get_self_addr() in seeds:
                return
            if len(self.liveEndpoints) == 0:
                self.sendGossip(message, seeds)
            else:
                prob = len(seeds) / float(len(self.liveEndpoints) + len(self.unreachableEndpoints))
                if random.random() <= prob:
                    self.sendGossip(message, seeds)

    def applyStateLocally(self, epStateMap):
        for ep, remoteState in epStateMap.items():
            if ep == self.message_manager.get_self_addr():
                continue

            if ep in self.endpointStateMap:
                localState = self.endpointStateMap[ep]
                localGen = localState.hbState.generation
                remoteGen = remoteState.hbState.generation
                localTime = time.time()

                logging.debug("{} local generation {}, remote generation {}".format(ep, localGen, remoteGen))

                if remoteGen > localTime:
                    # assume some peer has corrupted memory and is broadcasting an unbelievable generation
                    # about another peer (or itself)
                    s = "Received an invalid gossip generation for peer {}; local time = {}, received generation = {}"
                    logging.warning(s.format(ep, localTime, remoteGen))

                elif remoteGen == localGen:
                    # find maximum state
                    localMaxVersion = localState.getMaxEndpointStateVersion()
                    remoteMaxVersion = remoteState.getMaxEndpointStateVersion()

                    if remoteMaxVersion > localMaxVersion:
                        # apply states, but do not notify since there is no major change
                        self.applyNewStates(ep, remoteState)

                    else:
                        pattern = "Ignoring remote version {} <= {} for {}"
                        logging.debug(pattern.format(remoteMaxVersion, localMaxVersion, ep))

                else:
                    logging.debug("Ignoring remote generation {} < {}".format(remoteGen, localGen))

            else:
                # this is a new node
                self.handleMajorStateChange(ep, remoteState)

    def handleMajorStateChange(self, ep, epState):
        """
        This method is called whenever there is a "big" change in ep state (a generation change for a known node).
        :param ep:      endpoint
        :param epState: EndpointState for the endpoint
        """
        logging.debug("Adding endpoint state for {}".format(ep))
        self.endpointStateMap[ep] = epState
        self.liveEndpoints.append(ep)
        self.unreachableEndpoints.pop(ep)
        self.send_alive_notification(ep)

    def send_alive_notification(self, ep):
        msg = MESSAGE_TYPES[MESSAGE_CODE_NEW_CONNECTION](bytes(ep))
        self.message_manager.send_notification(msg)

    def applyNewStates(self, address, remoteState):
        oldVersion = self.endpointStateMap[address].hbState.version
        local = self.endpointStateMap[address]
        local.hbState = copy.deepcopy(remoteState.hbState)
        logging.debug("Updating heartbeat state version to {} from {} for {} ...".format(
                         self.endpointStateMap[address].hbState.version, oldVersion, address))

        assert remoteState.hbState.generation == local.hbState.generation
        local.addApplicationStates(remoteState.applicationStates)

        return self.endpointStateMap[address]

    def examineGossiper(self, gDigests):
        deltaGossipDigestList = set([])
        deltaEpStateMap = {}

        # Here we need to fire a GossipDigestAckMessage. If we have some data associated with this endpoint locally
        # then we follow the "if" path of the logic. If we have absolutely nothing for this endpoint we need to
        # request all the data for this endpoint.
        for gDigest in gDigests:

            if gDigest.endpoint in self.endpointStateMap:
                epState = self.endpointStateMap[gDigest.endpoint]
                maxLocalVersion = epState.getMaxEndpointStateVersion()

                if gDigest.generation == epState.hbState.generation and maxLocalVersion == gDigest.maxVersion:
                    continue

                if gDigest.generation > epState.hbState.generation:
                    # request everything from the gossiper
                    deltaGossipDigestList.add(self.requestAll(gDigest))

                elif gDigest.generation > epState.hbState.generation:
                    deltaEpStateMap = {**deltaEpStateMap, **self.sendAll(gDigest, 0)}

                else:
                    # If the max remote version is lesser, then we send all the data we have locally for this endpoint
                    # with version greater than the max remote version.
                    if maxLocalVersion > gDigest.maxVersion:
                        deltaEpStateMap = {**deltaEpStateMap, **self.sendAll(gDigest, gDigest.maxVersion)}

                    # If the max remote version is greater then we request the remote endpoint send us all the data
                    # for this endpoint with version greater than the max version number we have locally for this
                    # endpoint.
                    elif maxLocalVersion < gDigest.maxVersion:
                        deltaGossipDigestList.add(GossipDigest(gDigest.endpoint, gDigest.maxVersion, maxLocalVersion))

            else:
                # We are here since we have no data for this endpoint locally so request everything.
                deltaGossipDigestList.add(self.requestAll(gDigest))

        return deltaGossipDigestList, deltaEpStateMap

    def requestAll(self, gDigest):
        '''
        Request all the state for the endpoint in the gDigest
        '''
        logging.debug("requestAll for {}".format(gDigest.endpoint))
        return GossipDigest(gDigest.endpoint, gDigest.generation, 0)

    def sendAll(self, gDigest, maxRemoteVersion):
        '''
        Send all the data with version greater than maxRemoteVersion
        '''
        local = self.getStateForVersionBiggerThan(gDigest.endpoint, maxRemoteVersion)
        if local is not None:
            return {gDigest.endpoint: local}

    def getStateForVersionBiggerThan(self, endpoint, version):
        if endpoint in self.endpointStateMap:
            epState = self.endpointStateMap[endpoint]
            ret, states = None, {}

            if epState.hbState.version > version:
                hb = HeartBeatState(epState.hbState.generation, epState.hbState.version)

            for key, versioned_value in epState.applicationStates.items():
                if versioned_value.version > version:
                    if ret is None:
                        hb = HeartBeatState(epState.hbState.generation, epState.hbState.version)

                    states[key] = versioned_value
                    logging.debug("Adding state {}: {}".format(key, versioned_value.value))

            if hb is not None:
                return EndpointState(hb, states)

    def interval_task(self):
        '''
        this function will run every second
        '''
        try:
            hbState = self.endpointStateMap[self.message_manager.get_self_addr()].hbState
            hbState.updateHeartBeat()
            # TODO update other information of mine.
            logging.debug("My heartbeat is now {}".format(hbState.version))
            gDigests = self.makeRandomGossipDigest()
            if len(gDigests) > 0:
                message = GossipDigestSyn(gDigests).serialize()
                gossipedToSeed = self.doGossipToLiveMember(message)
                self.maybeGossipToUnreachableMember(message)
                if not gossipedToSeed or len(self.liveEndpoints) < len(self.message_manager.get_seeds()):
                    self.maybeGossipToSeed(message)
                    # self.doStatusCheck()
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
            handlers[msg['type']](msg['message'], msg['remote_identifier'])

    def new_connection_handler(self, msg, remote_identifier):
        logging.debug("new connection from {}, add it to liveEndpoints list".format(msg.remote_identifier))
        self.liveEndpoints.add(remote_identifier)
        # logging.debug("Current liveEndPoints is {}".format(str(self.liveEndpoints)))

    def connection_lost_handler(self, msg, remote_identifier):
        logging.debug("connection lost from {}, remove from liveEndpoints to unreachableEndpoints".format(remote_identifier))
        self.liveEndpoints.remove(remote_identifier)
        self.unreachableEndpoints[remote_identifier] = time.time()
        # logging.debug("current liveEndPoints is {}".format(str(self.liveEndpoints)))
        # logging.debug("current unreachableEndpoints is {}".format(str(self.unreachableEndpoints)))

    def gossip_handler(self, msg, remote_identifier):
        if self.firstSynSendAt == 0:
            logging.debug("ignore message because we have not send any syn message yes.")
            return
        msg_cls = Serializable.deserialize(msg.data)

        logging.debug('Received an gossip message {} from {}'.format(msg_cls, remote_identifier))

        if isinstance(msg_cls, GossipDigestSyn):
            deltaGossipDigestList, deltaEpStateMap = self.examineGossiper(msg_cls.gDigests)
            gossipDigestAck = GossipDigestAck(deltaGossipDigestList, deltaEpStateMap)
            logging.debug('sending gossip digest ack {} to {}'.format(gossipDigestAck, remote_identifier))
            self.message_manager.send_gossip_msg(remote_identifier, gossipDigestAck.serialize())

        elif isinstance(msg_cls, GossipDigestAck):
            if len(msg_cls.epStateMap) > 0:
                if time.time() < self.firstSynSendAt or self.firstSynSendAt == 0:
                    logging.debug("Ignoring unrequested GossipDigestAck from {}".format(remote_identifier))
                self.applyStateLocally(msg_cls.epStateMap)

            # Get the state required to send to this gossipee - construct GossipDigestAck2Message
            deltaEpStateMap = {}
            for gDigest in msg_cls.gDigests:
                state = self.getStateForVersionBiggerThan(gDigest.endpoint, gDigest.maxVersion)

                if state is not None:
                    deltaEpStateMap[gDigest.endpoint] = state

            if len(deltaEpStateMap) > 0:
                logging.debug('Sending a GossipDigestAck2Message to {}'.format(remote_identifier))
                self.message_manager.send_gossip_msg(remote_identifier, GossipDigestAck2(deltaEpStateMap).serialize())

            else:
                logging.debug('No state is newer, not send GossipDigestAck2Message to {}'.format(remote_identifier))

        elif isinstance(msg_cls, GossipDigestAck2):
            self.applyStateLocally(msg_cls.epStateMap)
