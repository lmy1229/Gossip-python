from gossip.gossip.state import EndPointStateMapSerializer, EndPointStateMapDeserializer

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

    def serialize(self):
        return '-'.join(map(str, [self.endpoint, self.generation, self.maxVersion]))

    @classmethod
    def deserialize(cls_obj, data):
        l = data.split('-')
        return cls_obj(l[0], l[1], l[2])

class GossipDigestSyn(object):
    '''
    This is the first message that gets sent out as a start of the Gossip protocol in a
    round.
    '''

    def __init__(self, gDigests):
        self.gDigests = gDigests  # list

    def serialize(self):
        return ' '.join([digest.serialize() for digest in self.gDigests])

    @classmethod
    def deserialize(cls_obj, data):
        l = data.split(' ')
        gDigests = [GossipDigest.deserialize(s) for s in data.split(' ')]
        return cls_obj(gDigests)

class GossipDigestAck(object):
    """
    This ack gets sent out as a result of the receipt of a GossipDigestSynMessage by an
    endpoint. This is the 2 stage of the 3 way messaging in the Gossip protocol.

    serialization rule:
    <GossipDigests>\n
    ep1name-<EndPointState>\n
    ep2name-<EndPointState>

    for example:
    127.0.0.1:7001-12341-12 127.0.0.1:7002-13412-1\n
    127.0.0.1:7001-[STATUS 1, version 12]/[LOAD 2, version 12]/[HeartBeat, generation 12341, version 12]\n
    127.0.0.1:7002-[STATUS 2, version 11]/[LOAD 2, version 11]/[HeartBeat, generation 13411, version 12]
    """
    def __init__(self, gDigests, epStateMap):
        self.gDigests = gDigests
        self.epStateMap = epStateMap

    def serialize(self):
        gDigests_serial = ' '.join([digest.serialize for digest in self.gDigests])
        epStateMap_serial = EndPointStateMapSerializer(self.epStateMap)
        return '\n'.join([gDigests_serial, epStateMap_serial])

    @classmethod
    def deserialize(cls_obj, data):
        
        comps = data.split('\n')
        gDigests_serial = comps[0]
        epStateMap_serial =comps[1:]
        gDigests = [GossipDigest.deserialize(s) for s in gDigests_serial.split(' ')]
        epStateMap = EndPointStateMapDeserializer(epStateMap_serial)
        return cls_obj(gDigests, epStateMap)

class GossipDigestAck2:
    """
    This ack gets sent out as a result of the receipt of a GossipDigestAckMessage. This the
    last stage of the 3 way messaging of the Gossip protocol.
    """

    def __init__(self, epStateMap):
        self.epStateMap = epStateMap

    def serialize(self):
        return EndPointStateMapSerializer(self.epStateMap)

    @classmethod
    def deserialize(cls_obj, data):
        return cls_obj(EndPointStateMapDeserializer(data))