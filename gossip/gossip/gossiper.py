import socket
import sys
import time
import random
import logging

from gossip.state import ApplicationState, EndpointState
from gossip.GossipDigest import GossipDigest

class Gossiper(object):

    def __init__(self):
        self.QUARANTINE_DELAY_IN_Millis = 30000
        # Maximimum difference between generation value and local time we are willing to accept about a peer
        self.MAX_GENERATION_DIFFERENCE = 86400 * 365
        self.STATES = [e.value for e in ApplicationState]
        self.intervalInMillis = 1000
        self.liveEndpoints = []  # lists
        self.unreachableEndpoints = {}  # map from address to time interval
        self.seeds = []
        self.endpointStateMap = {}  # map from address to EndpointState
        # map where key is endpoint and value is timestamp when this endpoint was removed from
        # gossip. We will ignore any gossip regarding these endpoints for QUARANTINE_DELAY time
        # after removal to prevent nodes from falsely reincarnating during the time when removal
        # gossip gets propagated to all nodes
        self.justRemovedEndpoints = {}

        self.expireTimeEndpointMap = {}
        # Timestamp to prevent processing any in-flight messages for we've not send any SYN yet
        self.firstSynSendAt = 0L
        self.lastProcessedMessageAt = time.time()

    def doGossipToLiveMember(self, message):
        if len(self.liveEndpoints) == 0:
            return False
        return self.sendGossip(message, self.liveEndpoints)

    def maybeGossipToUnreachableMember(self, message):
        liveEndpointCount = len(self.liveEndpoints)
        unreachableEndpointCount = len(self.unreachableEndpoints)
        if unreachableEndpointCount > 0:
            prob = float(unreachableEndpointCount) / (liveEndpointCount + 1)
            if random.random() < prob:
                self.sendGossip(message, self.unreachableEndpoints.keys())

    def maybeGossipToSeed(self, message):
        size = self.seeds.size()
        if size > 0:
            if size == 1 and self.get_current_address() in self.seeds:  # TODO, current_address
                return
            if len(self.liveEndpoints) == 0:
                self.sendGossip(message, self.seeds)
            else:
                prob = size / float(len(self.liveEndpoints) + len(self.unreachableEndpoints))
                if random.random() <= prob:
                    self.sendGossip(message, self.seeds)

    def sendGossip(self, message, endpoints):
        size = endpoints.size()
        if size < 1:
            return False
        to = self.liveEndpoints[random.randint(0, size)]
        if self.firstSynSendAt == 0:
            self.firstSynSendAt = time.time()
        # send(message, to)  # TODO implement function send
        return (to in self.seeds)

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

    def applyStateLocally(self, epStateMap):
        for ep in epStateMap:
            if ep == self.get_current_address():
                continue
            if ep in self.justRemovedEndpoints:
                logging.debug('Ignoring gossip for {} because it is quarantined'.format(ep))
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

                    if not localState.isAlive() and not self.isDeadState(localState):  # TODO
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

        assert remoteState.hbState.generation == localState.hbState.generation
        localState.addApplicationStates(remoteState.applicationStates)  # TODO

        for key in remoteState.applicationStates:
            self.doOnChangeNotifications(address, key, remoteState.applicationStates[key])  # TODO
        return self.endpointStateMap[address]



    def get_current_address(self):
        pass  # TODO

    def run(self):
        '''
        TODO
        '''


gossiper = Gossiper()
