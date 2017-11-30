

class GossipDigest():
    """docstring for GossipDigest"""
    def __init__(self, ep, gen, ver):
        self.endpoint = ep
        self.generation = gen
        self.version = ver

    def toString(self):
        return ':'.join([self.endpoint, self.generation, self.version])

    @classmethod
    def deserialize(data):
        pass
