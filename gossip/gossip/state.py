from enum import Enum, auto
from datetime import datetime


class VersionedValue(object):

    def __init__(self, value, version):
        self.value = value
        self.version = version


class HeartBeatState(object):

    def __init__(self, gen, ver=0):
        self.generation = gen
        self.version = ver

    def __str__(self):
        return "HeartBeat: generation = {}, version = {}".format(self.generation, self.version)


class ApplicationState(Enum):
    # TODO May be some are useless
    STATUS = auto()
    LOAD = auto()
    SCHEMA = auto()
    DC = auto()
    RACK = auto()
    RELEASE_VERSION = auto()
    REMOVAL_COORDINATOR = auto()
    INTERNAL_IP = auto()
    RPC_ADDRESS = auto()
    X_11_PADDING = auto()  # padding specifically for 1.1
    SEVERITY = auto()
    NET_VERSION = auto()
    HOST_ID = auto()
    TOKENS = auto()
    RPC_READY = auto()
    # pad to allow adding new states to existing cluster
    X1 = auto()
    X2 = auto()
    X3 = auto()
    X4 = auto()
    X5 = auto()
    X6 = auto()
    X7 = auto()
    X8 = auto()
    X9 = auto()
    X10 = auto()


class EndpointState(object):

    def __init__(self, hbState, applicationStates={}):
        self.hbState = hbState
        self.applicationStates = applicationStates
        # fields below do not get serialized
        self.isAlive = True
        self.updateTimestamp = datetime.now()

    def getApplicationState(self, key):
        return self.applicationStates[key]

    def addApplicationState(self, key, value):
        self.applicationStates[key] = value

    def addApplicationStates(self, applicationStates):
        self.applicationStates = {**self.applicationStates, **applicationStates}

    def getMaxEndpointStateVersion(self):
        """Return either: the greatest heartbeat or application state
        """
        return max([st.version for st in self.applicationStates.values()] +
                   [self.hbState.version])
