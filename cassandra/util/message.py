import struct
import json
from cassandra.util.packing import short_to_bytes, bytes_to_short, addr_to_bytes
from cassandra.util.message_codes import *


class Message:
    """ basic class of message
    PROPERTIES:
    - code(int): code of the message, referring to message_codes.py
    - data(bytes): payload of the message, in bytes
    - source_addr((str, int)): addr and port of the source of this message
    """
    def __init__(self, code, data, source_addr):
        self.code = code
        self.data = data
        self.source_addr = source_addr
        self.retry_counter = 0

    def encode(self):

        if not self.source_addr:
            raise Exception('Message: source_addr not set')

        size = len(self.data) + 6  # add 6 for addr and port
        b_size = short_to_bytes(size)
        b_code = short_to_bytes(self.code)
        b_source_addr = addr_to_bytes(self.source_addr)
        b_msg = b_size + b_code + b_source_addr + self.data
        return b_msg

    def get_values(self):
        raise Exception('Not Implemented')

    def get_source(self):
        return self.source_addr

    def __str__(self):
        return '%s' % self.get_values()


class NewConnectionMessage(Message):
    def __init__(self, data, source_addr=None):
        super(NewConnectionMessage, self).__init__(MESSAGE_CODE_NEW_CONNECTION, data, source_addr)
        self.remote_identifier = data.decode()

    def get_values(self):
        return {'code': self.code, 'remote_identifier': self.remote_identifier, 'source': self.source_addr, 'retry': self.retry_counter}


class NewConnectionHandShakeMessage(Message):
    def __init__(self, _, source_addr=None):
        super(NewConnectionHandShakeMessage, self).__init__(MESSAGE_CODE_NEW_CONNECTION_HANDSHAKE, bytes(), source_addr)

    def get_values(self):
        return {'code': MESSAGE_CODE_NEW_CONNECTION_HANDSHAKE, 'source_addr': self.source_addr}


class NewLiveNodeMessage(Message):

    def __init__(self, data, source_addr=None):
        if source_addr is None:
            source_addr = data.decode()
        super(NewLiveNodeMessage, self).__init__(MESSAGE_CODE_NEW_LIVE_NODE, data, source_addr)
        self.remote_identifier = data.decode()

    def get_values(self):
        return {'code': self.code, 'remote_identifier': self.remote_identifier, 'source': self.source_addr}


class LostLiveNodeMessage(Message):

    def __init__(self, data, source_addr=None):
        if source_addr is None:
            source_addr = data.decode()
        super(LostLiveNodeMessage, self).__init__(MESSAGE_CODE_LOST_LIVE_NODE, data, source_addr)
        self.remote_identifier = data.decode()

    def get_values(self):
        return {'code': self.code, 'remote_identifier': self.remote_identifier, 'source': self.source_addr}


class ConnectionLostMessage(Message):
    def __init__(self, data, source_addr=None):
        super(ConnectionLostMessage, self).__init__(MESSAGE_CODE_CONNECTION_LOST, data, source_addr)
        self.remote_identifier = data.decode()

    def get_values(self):
        return {'code': self.code, 'remote_identifier': self.remote_identifier, 'source': self.source_addr}


class GossipMessage(Message):
    def __init__(self, data, source_addr=None):
        super(GossipMessage, self).__init__(MESSAGE_CODE_GOSSIP, data, source_addr)

    def get_values(self):
        return {'code': self.code, 'message': self.data, 'source': self.source_addr}


class RegistrationMessage(Message):
    def __init__(self, data, source_addr=None):
        super(RegistrationMessage, self).__init__(MESSAGE_CODE_REGISTRATION, data, source_addr)
        code_hi, code_lo = struct.unpack('2B', data[0:2])
        self.register_code = bytes_to_short(code_hi, code_lo)
        self.register_identity = data[2:len(data)].decode()

    def get_values(self):
        return {
            'code': self.code,
            'message': self.data,
            'register_code': self.register_code,
            'register_identity': self.register_identity
        }

    def encode(self):
        raise Exception('RegistrationMessage should not be encoded.')


class RequestMessage(Message):
    def __init__(self, data, source_addr=None):
        super(RequestMessage, self).__init__(MESSAGE_CODE_REQUEST, data, source_addr)
        raw = json.loads(data.decode())
        self.request = tuple(raw['request'])
        self.request_hash = raw['request_hash']

    def get_values(self):
        return {
            'code': self.code,
            'request': self.request,
            'source': self.source_addr,
            'request_hash': self.request_hash}


class ResponseMessage(Message):
    def __init__(self, data, source_addr=None):
        super(ResponseMessage, self).__init__(MESSAGE_CODE_RESPONSE, data, source_addr)
        raw = json.loads(data.decode())
        self.status = raw['status']
        self.description = raw['description']
        self.request_hash = raw['request_hash']

    def get_values(self):
        return {
            'code': self.code,
            'status': self.status,
            'description': self.description,
            'source': self.source_addr,
            'request_hash': self.request_hash}


MESSAGE_TYPES = {
    MESSAGE_CODE_GOSSIP: GossipMessage,
    MESSAGE_CODE_REGISTRATION: RegistrationMessage,
    MESSAGE_CODE_NEW_CONNECTION: NewConnectionMessage,
    MESSAGE_CODE_CONNECTION_LOST: ConnectionLostMessage,
    MESSAGE_CODE_NEW_CONNECTION_HANDSHAKE: NewConnectionHandShakeMessage,
    MESSAGE_CODE_LOST_LIVE_NODE: LostLiveNodeMessage,
    MESSAGE_CODE_NEW_LIVE_NODE: NewLiveNodeMessage,
    MESSAGE_CODE_REQUEST: RequestMessage,
    MESSAGE_CODE_RESPONSE: ResponseMessage,
}
