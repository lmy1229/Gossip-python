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
    def deserialize(cls, data):
        comp = [c.strip() for c in data.strip().split(',')]
        if comp[0] == "HeartBeat" and comp[1].startswith("generation") and comp[2].startswith("version"):
            return cls(int(comp[1].split(' ')[1]), int(comp[2].split(' ')[1]))
        else:
            return None

    def update_heart_beat(self):
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
    def get_description(cls, state):
        return cls(state).name

    @classmethod
    def get_state(cls, description):
        for state in cls:
            if state.name == description:
                return state
        return cls.UNKNOWN


class EndpointState(object):
    def __init__(self, hb_state=None, application_states=None):
        if application_states is None:
            application_states = {}
        if hb_state is None:
            hb_state = HeartBeatState()
        self.hbState = hb_state
        self.applicationStates = application_states  # from key to versioned value
        # fields below do not get serialized
        self.isAlive = True
        self.updateTimestamp = datetime.now()

    def get_application_state(self, key):
        return self.applicationStates[key]

    def add_application_state(self, key, value):
        self.applicationStates[key] = value

    def add_application_states(self, application_states):
        self.applicationStates = {**self.applicationStates, **application_states}

    def get_max_endpoint_state_version(self):
        """Return either: the greatest heartbeat or application state
        """
        return max([st.version for st in self.applicationStates.values()] +
                   [self.hbState.version])

    def serialize(self):
        hb_serial = '[%s]' % self.hbState.serialize()
        app_states_serial = []
        for key, value in self.applicationStates.items():
            s = '[%s %s, version %d]' % (ApplicationState.get_description(key), value.value, value.version)
            app_states_serial.append(s)
        serial = '/'.join(app_states_serial + [hb_serial])
        return serial

    @classmethod
    def deserialize(cls, data):
        l = [comp.strip()[1:-1] for comp in data.strip().split('/')]
        hb_state = HeartBeatState.deserialize(l[-1])
        app_states = {}
        for state in l[:-1]:
            s, v = [comp.strip().split(' ') for comp in state.split(',')]
            s_name, s_value = s
            _, ver = v
            ver = int(ver)
            app_states[ApplicationState.get_state(s_name).value] = VersionedValue(s_value, ver)
        return cls(hb_state, app_states)


def endpoint_state_map_serializer(endpoint_state_map):
    return '\n'.join(['%s-%s' % (k, v.serialize()) for k, v in endpoint_state_map.items()])


def end_point_state_map_deserializer(data):
    endpoint_state_map = {}
    if len(data.strip()) == 0:
        return {}
    for comp in data.strip().split('\n'):
        endpoint_name, eps_serial = comp.split('-')
        endpoint_state_map[endpoint_name] = EndpointState.deserialize(eps_serial)
    return endpoint_state_map
