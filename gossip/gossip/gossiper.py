import socket
import sys
import time
import random
import logging
from multiprocessing import Process

from gossip.gossip.state import ApplicationState, EndpointState, VersionedValue
from gossip.gossip.GossipDigest import GossipDigest, GossipDigestSyn
from gossip.util.message_codes import  MESSAGE_CODE_NEW_CONNECTION
from gossip.util.message_codes import  MESSAGE_CODE_CONNECTION_LOST
from gossip.util.message_codes import  MESSAGE_CODE_GOSSIP


class Gossiper(object):

    def __init__(self, manager):
        self.QUARANTINE_DELAY_IN_Millis = 30000
        # Maximimum difference between generation value and local time we are willing to accept about a peer
        self.MAX_GENERATION_DIFFERENCE = 86400 * 365
        self.STATES = [e.value for e in ApplicationState]
        self.intervalInMillis = 1000
        self.liveEndpoints = []  # lists
        self.unreachableEndpoints = {}  # map from address to time interval
        self.seeds = []  # TODO
        self.endpointStateMap = {}  # map from address to EndpointState
        # map where key is endpoint and value is timestamp when this endpoint was removed from
        # gossip. We will ignore any gossip regarding these endpoints for QUARANTINE_DELAY time
        # after removal to prevent nodes from falsely reincarnating during the time when removal
        # gossip gets propagated to all nodes
        self.justRemovedEndpoints = {}

        self.expireTimeEndpointMap = {}
        # Timestamp to prevent processing any in-flight messages for we've not send any SYN yet
        self.firstSynSendAt = 0
        self.lastProcessedMessageAt = time.time()

        self.manager = manager

    def isShutdown(self, endpoint):
        if endpoint not in self.endpointStateMap:
            return False
        epState = self.endpointStateMap[endpoint];
        if ApplicationState.STATUS not in epState:
            return False
        value = epState[ApplicationState.STATUS]
        pieces = value.split(VersionedValue.DELIMITER_STR, -1)  # TODO
        assert (pieces.length > 0)
        state = pieces[0]
        return state.equals(VersionedValue.SHUTDOWN)  # TODO STATUS值的定义

    def markAsShutdown(self, endpoint):
        if endpoint not in self.endpointStateMap:
            return
        epState = self.endpointStateMap[endpoint]
        #epState.addApplicationState(ApplicationState.STATUS, ValueFactory.shutdown(True))  # TODO
        #epState.addApplicationState(ApplicationState.RPC_READY, ValueFactory.rpcReady(False))  # TODO
        #epState.hbState.forceHighestPossibleVersionUnsafe();
        self.markDead(endpoint, epState)
        # FailureDetector.instance.forceConviction(endpoint); TODO

    def evictFromMembership(self, endpoint):
        """
        Removes the endpoint from gossip completely
        :param endpoint: endpoint to be removed from the current membership.
        :return: None
        """
        self.unreachableEndpoints.pop(endpoint, None)
        self.endpointStateMap.pop(endpoint, None)
        self.expireTimeEndpointMap.pop(endpoint, None)
        # FailureDetector.instance.remove(endpoint);
        # quarantineEndpoint(endpoint);
        logging.debug("evicting {} from gossip".format(endpoint))

    def removeEndpoint(self, endpoint):
        """
        Removes the endpoint from Gossip but retains endpoint state
        :param endpoint:
        :return:
        """
        pass

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
        size = endpoints.size()
        if size < 1:
            return False
        to = self.liveEndpoints[random.randint(0, size)]

        logging.debug("Sending a GossipDigestSyn to {} ...".format(to))

        if self.firstSynSendAt == 0:
            self.firstSynSendAt = time.time()
        self.send(message, to)  # TODO implement function send
        return (to in self.seeds)

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

    def doOnChangeNotifications(self, address, key, remoteStateValue):
        pass

    def markAlive(self, ep, localState):
        pass

    def get_current_address(self):
        pass  # TODO

    def send(self, message, to):
        pass

    def doStatusCheck(self):
        pass

gossiper_instance = None


# TODO
class  GossiperTask(Process):

    def run(self):
        '''
        this function will run every second
        '''
        # lock()?
        try:
            hbState = gossiper_instance.endpointStateMap[gossiper_instance.get_current_address()].hbState
            hbState.updateHeartBeat()
            logging.debug("My heartbeat is now {}".format(hbState.version))
            gDigests = gossiper_instance.makeRandomGossipDigest()
            if len(gDigests) > 0:
                message = GossipDigestSyn(gDigests).serialize()
                gossipedToSeed = gossiper_instance.doGossipToLiveMember(message)
                gossiper_instance.maybeGossipToUnreachableMember(message)
                if not gossipedToSeed or len(gossiper_instance.liveEndpoints) < gossiper_instance.seeds.size():
                    gossiper_instance.maybeGossipToSeed(message)

            gossiper_instance.doStatusCheck()
        except Exception as e:
            logging.error("Gossip error: {}".format(e))
        finally:
            pass  # unlock?

def GossipierMessageReceiver(Process):

    def __init__(self, gossipier_instance):
        self.manager = gossiper_instance.manager
        self.receiver_queue = self.manager.receiver_queue
        self.sender_queue =  self.manager.sender_queue
        self.gossiper_instance = gossiper_instance
        self.handlers = {
            MESSAGE_CODE_NEW_CONNECTION: self.new_connection_handler,
            MESSAGE_CODE_CONNECTION_LOST: self.connection_lost_handler,
            MESSAGE_CODE_GOSSIP: self.gossip_handler,
        }

    def new_connection_handler(self, msg):
        pass

    def connection_lost_handler(self, msg):
        pass

    def gossip_handler(self, msg):
        pass

    def run(self):
        while True:
            msg = self.manager.get_msg()
            self.handlers[msg['type']](msg['message'])

