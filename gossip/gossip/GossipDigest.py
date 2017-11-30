

class GossipDigest():
    """Contains information about a specified list of Endpoints and the largest version
       of the state they have generated as known by the local endpoint.
    """

    def __init__(self, ep, gen, ver):
        self.endpoint = ep
        self.generation = gen
        self.maxVersion = ver

    def __lt__(self, other):
        return self.generation > other.generation or \
               (self.generation == other.generation and self.maxVersion > other.maxVersion)


class GossipDigestSyn(object):
    '''
    This is the first message that gets sent out as a start of the Gossip protocol in a
    round.
    '''

    def __init__(self, gDigests):
        self.gDigests = gDigests

    def serialize(self):
        pass


class GossipDigestAck(object):
    """
    This ack gets sent out as a result of the receipt of a GossipDigestSynMessage by an
    endpoint. This is the 2 stage of the 3 way messaging in the Gossip protocol.
    """
    def __init__(self, gDigests, epStateMap):
        self.gDigests = gDigests
        self.epStateMap = epStateMap


class GossipDigestAck2:
    """
    This ack gets sent out as a result of the receipt of a GossipDigestAckMessage. This the
    last stage of the 3 way messaging of the Gossip protocol.
    """

    def __init__(self, epStateMap):
        self.epStateMap = epStateMap
