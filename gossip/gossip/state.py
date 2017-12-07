from enum import Enum
from datetime import datetime
import time


class VersionedValue(object):

    def __init__(self, value, version):
        self.value = value
        self.version = version


class HeartBeatState(object):

    def __init__(self, gen=None, ver=0):
        if gen is None:
            gen = time.time()
        self.generation = gen
        self.version = ver

    def __str__(self):
        return "HeartBeat: generation = {}, version = {}".format(self.generation, self.version)

    def serialize(self):
        return "HeartBeat, generation %d, version %d" % (self.generation, self.version)

    @classmethod
    def deserialize(cls_obj, data):
        comp = [c.strip() for c in data.strip().split(',')]
        if comp[0] == "HeartBeat" and comp[1].startswith("generation") and comp[2].startswith("version"):
            return cls_obj(int(comp[1].split(' ')[1]), int(comp[2].split(' ')[1]))
        else:
            return None

    def updateHeartBeat(self):
        self.version += 1


class ApplicationState(Enum):
    # TODO Maybe some are useless
    STATUS = 1
    LOAD = 2
    SCHEMA = 3
    DC = 4
    RACK = 5
    RELEASE_VERSION = 6
    REMOVAL_COORDINATOR = 7
    INTERNAL_IP = 8
    RPC_ADDRESS = 9
    X_11_PADDING = 10  # padding specifically for 1.1
    SEVERITY = 11
    NET_VERSION = 12
    HOST_ID = 13
    TOKENS = 14
    RPC_READY = 15
    # pad to allow adding new states to existing cluster
    X1 = 16
    X2 = 17
    X3 = 18
    X4 = 19
    X5 = 20
    X6 = 21
    X7 = 22
    X8 = 23
    X9 = 24
    X10 = 25
    UNKNOWN = 26

    @classmethod
    def get_description(cls_obj, state):
        return cls_obj(state).name

    @classmethod
    def get_state(cls_obj, descrpition):
        for state in cls_obj:
            if state.name == descrpition:
                return state
        return cls_obj.UNKNOWN


class EndpointState(object):

    def __init__(self, hbState=None, applicationStates={}):
        if hbState is None:
            hbState = HeartBeatState()
        self.hbState = hbState
        self.applicationStates = applicationStates  # from key to versioned value
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

    def serialize(self):
        hb_serial = '[%s]' % self.hbState.serialize()
        appStates_serial = []
        for key, value in self.applicationStates.items():
            appStates_serial.append('[%s %s, version %d]' % (ApplicationState.get_description(key), value.value, value.version))
        serial = '/'.join(appStates_serial + [hb_serial])
        return serial

    @classmethod
    def deserialize(cls_obj, data):
        l = [comp.strip()[1:-1] for comp in data.strip().split('/')]
        hbState = HeartBeatState.deserialize(l[-1])
        appStates = {}
        for state in l[:-1]:
            s, v = [comp.strip().split(' ') for comp in state.split(',')]
            s_name, s_value = s
            _, ver = v
            ver = int(ver)
            appStates[ApplicationState.get_state(s_name).value] = VersionedValue(s_value, ver)
        return cls_obj(hbState, appStates)

def EndPointStateMapSerializer(epsm):
    return '\n'.join(['%s-%s' % (k, v.serialize()) for k, v in epsm.items()])

def EndPointStateMapDeserializer(data):
    epsm = {}
    if len(data.strip()) == 0:
        return {}
    for comp in data.strip().split('\n'):
        epname, eps_serial = comp.split('-')
        epsm[epname] = EndpointState.deserialize(eps_serial)
    return epsm
