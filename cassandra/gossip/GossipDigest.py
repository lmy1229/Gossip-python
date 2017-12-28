from cassandra.gossip.state import endpoint_state_map_serializer, end_point_state_map_deserializer
import json
import sys


def str_to_class(s):
    return getattr(sys.modules[__name__], s)


class Serializable(object):

    def __init__(self):
        pass

    def __str__(self):
        return json.dumps({
            "type": self.__class__.__name__,
            "params": self.export_to_serializable()
        })

    def export_to_serializable(self):
        raise NotImplementedError

    def serialize(self):
        return bytes(self.__str__(), 'ascii')

    @classmethod
    def construct_from_params(cls, d):
        raise NotImplementedError

    @classmethod
    def deserialize(cls, data):
        d = json.loads(data.decode())
        return str_to_class(d['type']).construct_from_params(d['params'])


class GossipDigest(Serializable):
    """Contains information about a specified list of Endpoints and the largest version
       of the state they have generated as known by the local endpoint.
    """

    def __init__(self, ep, gen, ver):
        # To handle multi type of remote identifier
        super(GossipDigest, self).__init__()
        if isinstance(ep, str):
            self.endpoint = ep
        elif len(ep) == 2:
            self.endpoint = "%s:%d" % (ep[0], ep[1])
        self.generation = gen
        self.maxVersion = ver

    def __lt__(self, other):
        return self.generation > other.generation or \
               (self.generation == other.generation and self.maxVersion > other.maxVersion)

    def export_to_serializable(self):
        return [self.endpoint, self.generation, self.maxVersion]

    @classmethod
    def construct_from_params(cls, params):
        return GossipDigest(*params)


class GossipDigestSyn(Serializable):
    """
    This is the first message that gets sent out as a start of the Gossip protocol in a
    round.
    """

    def __init__(self, g_digests):
        super(GossipDigestSyn, self).__init__()
        self.gDigests = g_digests  # list

    def export_to_serializable(self):
        return list(map(lambda x: x.export_to_serializable(), self.gDigests))

    @classmethod
    def construct_from_params(cls, params):
        return GossipDigestSyn(list(map(lambda x: GossipDigest.construct_from_params(x), params)))


class GossipDigestAck(Serializable):
    """
    This ack gets sent out as a result of the receipt of a GossipDigestSynMessage by an
    endpoint. This is the 2 stage of the 3 way messaging in the Gossip protocol.
    """
    def __init__(self, g_digests, ep_state_map):
        super(GossipDigestAck, self).__init__()
        self.gDigests = g_digests
        self.epStateMap = ep_state_map

    def export_to_serializable(self):
        return {
            "gDigests": [gDigest.export_to_serializable() for gDigest in self.gDigests],
            "epStateMap": endpoint_state_map_serializer(self.epStateMap)
        }

    @classmethod
    def construct_from_params(cls, params):
        return GossipDigestAck([GossipDigest.construct_from_params(s) for s in params["gDigests"]],
                               end_point_state_map_deserializer(params['epStateMap']))


class GossipDigestAck2(Serializable):
    """
    This ack gets sent out as a result of the receipt of a GossipDigestAckMessage. This the
    last stage of the 3 way messaging of the Gossip protocol.
    """

    def __init__(self, ep_state_map):
        super(GossipDigestAck2, self).__init__()
        self.epStateMap = ep_state_map

    def export_to_serializable(self):
        return endpoint_state_map_serializer(self.epStateMap)

    @classmethod
    def construct_from_params(cls, params):
        return GossipDigestAck2(end_point_state_map_deserializer(params))
