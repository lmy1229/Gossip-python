import time
import random
import logging
from gossip.gossip.state import EndpointState
from gossip.gossip.GossipDigest import GossipDigest, GossipDigestSyn
from gossip.util.message_codes import MESSAGE_CODE_NEW_CONNECTION
from gossip.util.message_codes import MESSAGE_CODE_CONNECTION_LOST
from gossip.util.message_codes import MESSAGE_CODE_GOSSIP
from gossip.util.scheduler import Scheduler
from pprint import pprint


class Gossiper(Scheduler):

    def __init__(self, message_manager):
        self.message_manager = message_manager
        # Maximimum difference between generation value and local time we are willing to accept about a peer
        self.interval = 1
        self.liveEndpoints = []  # address lists
        self.unreachableEndpoints = {}  # map from address to time interval
        self.endpointStateMap = {self.message_manager.get_self_addr(): EndpointState()}  # map from address to EndpointState
        # Timestamp to prevent processing any in-flight messages for we've not send any SYN yet
        self.firstSynSendAt = 0

        super(Gossiper, self).__init__(self.interval)

    def makeRandomGossipDigest(self):
        endpoints = self.endpointStateMap.keys()
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
        self.message_manager.send_gossip_msg(to, message.serialize())
        return (to in self.message_manager.seeds)

    def doGossipToLiveMember(self, message):
        return self.sendGossip(message, self.liveEndpoints)

    def maybeGossipToUnreachableMember(self, message):
        if len(self.unreachableEndpoints) > 0:
            if random.random() < float(len(self.unreachableEndpoints)) / (len(self.liveEndpoints) + 1):
                self.sendGossip(message, self.unreachableEndpoints.keys())

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
        for ep in epStateMap:
            if ep == self.message_manager.get_self_addr():
                continue

            remoteState = epStateMap[ep]
            if ep in self.endpointStateMap:
                localState = self.endpointStateMap[ep]
                localGen = localState.hbState.generation
                remoteGen = remoteState.hbState.generation
                localTime = time.time()  # TODO

                logging.debug("{} local generation {}, remote generation {}".format(ep, localGen, remoteGen))

                if remoteGen > localTime + self.MAX_GENERATION_DIFFERENCE:
                    # assume some peer has corrupted memory and is broadcasting an unbelievable generation
                    # about another peer (or itself)
                    s = "received an invalid gossip generation for peer {}; local time = {}, received generation = {}"
                    logging.warning(s.format(ep, localTime, remoteGen))
                elif remoteGen == localGen:
                    # find maximum state
                    localMaxVersion = localState.getMaxEndpointStateVersion()
                    remoteMaxVersion = remoteState.getMaxEndpointStateVersion()
                    if remoteMaxVersion > localMaxVersion:
                        # apply states, but do not notify since there is no major change
                        localState = self.applyNewStates(ep, remoteState)  # TODO
                    else:
                        logging.debug("Ignoring remote version {} <= {} for {}".format(remoteMaxVersion,
                                                                                       localMaxVersion,
                                                                                       ep))

                    if not localState.isAlive and not self.isDeadState(localState):  # TODO
                        self.markAlive(ep, localState)  # TODO
                else:
                    logging.debug("Ignoring remote generation {} < {}", remoteGen, localGen)
            else:
                # this is a new node, report it to the FD in case it is the first time we are seeing it
                # AND it's not alive
                # FailureDetector.instance.report(ep)  # TODO
                self.handleMajorStateChange(ep, remoteState)

    def applyNewStates(self, address, remoteState):
        # don't assert here, since if the node restarts the version will go back to zero
        oldVersion = self.endpointStateMap[address].hbState.version
        self.endpointStateMap[address].hbState = remoteState.hbState
        logging.debug("Updating heartbeat state version to {} from {} for {} ...".format(
                         self.endpointStateMap[address].hbState.version, oldVersion, address))

        assert remoteState.hbState.generation == self.endpointStateMap[address].hbState.generation
        self.endpointStateMap[address].addApplicationStates(remoteState.applicationStates)  # TODO

        for key in remoteState.applicationStates:
            self.doOnChangeNotifications(address, key, remoteState.applicationStates[key])  # TODO
        return self.endpointStateMap[address]

    def interval_task(self):
        '''
        this function will run every second
        '''
        try:
            hbState = self.endpointStateMap[self.message_manager.get_self_addr()].hbState
            hbState.updateHeartBeat()
            logging.debug("My heartbeat is now {}".format(hbState.version))
            gDigests = self.makeRandomGossipDigest()
            if len(gDigests) > 0:
                message = GossipDigestSyn(gDigests).serialize()
                gossipedToSeed = self.doGossipToLiveMember(message)
                self.maybeGossipToUnreachableMember(message)
                if not gossipedToSeed or len(self.liveEndpoints) < len(self.seeds):
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
            print('receive message:')
            pprint(msg)
            handlers[msg['type']](msg['message'])

    def new_connection_handler(self, msg):
        print("new connection")

    def connection_lost_handler(self, msg):
        print("connection loss")

    def gossip_handler(self, msg):
        print("gossip message")
