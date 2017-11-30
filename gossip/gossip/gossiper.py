import socket
import sys
import time
import random

from gossip.state import ApplicationState, EndpointState


class Gossiper(object):

    def __init__(self, intervalInMillis=1000, ):
        self.STATES = [e.value for e in ApplicationState]
        liveEndpoints = []  # lists
        unreachableEndpoints = {}  # map from address to time interval
        seeds = []
        endpointStateMap = {}  # map from address to EndpointState
        # Timestamp to prevent processing any in-flight messages for we've not send any SYN yet, see CASSANDRA-12653.
        firstSynSendAt = 0L
        lastProcessedMessageAt = time.time()

    def doGossipToLiveMember(self, message):
        if len(self.liveEndpoints) == 0:
            return False
        return self.sendGossip(message, liveEndpoints)

    def maybeGossipToUnreachableMember(self, message):
        liveEndpointCount = len(self.liveEndpoints)
        unreachableEndpointCount = len(self.unreachableEndpoints)
        if unreachableEndpointCount > 0:
            prob = float(unreachableEndpointCount) / (liveEndpointCount + 1)
            if random.random() < prob:
                self.sendGossip(message, unreachableEndpoints.keys())

    def maybeGossipToSeed(message):
        size = self.seeds.size()
        if size > 0:
            if size == 1 and current_address in seeds:  # TODO, current_address
                return
            if len(self.liveEndpoints) == 0:
                self.sendGossip(message, self.seeds)
            else:
                prob = size / float(len(self.liveEndpoints) + len(self.unreachableEndpoints))
                if random.random() <= prob:
                    sendGossip(message, seeds)

    def sendGossip(self, message, endpoints):
        size = endpoints.size()
        if size < 1:
            return False
        to = liveEndpoints[random.randint(0, size)]
        if self.firstSynSendAt == 0:
            self.firstSynSendAt = time.time()
        send(message, to)  # TODO implement function send
        return (to in seeds)
