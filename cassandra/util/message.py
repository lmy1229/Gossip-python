import struct

from gossip.util.packing import short_to_bytes, bytes_to_short
from gossip.util.message_codes import *

class Message():
    """ basic class of message """
    def __init__(self, code, data):
        self.code = code
        self.data = data

    def encode(self):
        size = len(self.data)
        b_size = short_to_bytes(size)
        b_code = short_to_bytes(self.code)
        b_msg = b_size + b_code + self.data
        return b_msg

    def get_values(self):
        raise Exception('Not Implemented')

    def __str__(self):
        return '%s' % self.get_values()
        
class NewConnectionMessage(Message):
    def __init__(self, data):
        super().__init__(MESSAGE_CODE_NEW_CONNECTION, data)
        self.remote_identifier = data.decode()

    def get_values(self):
        return {'code': self.code, 'remote_identifier': self.remote_identifier}

class ConnectionLostMessage(Message):
    def __init__(self, data):
        super().__init__(MESSAGE_CODE_CONNECTION_LOST, data)
        self.remote_identifier = data.decode()

    def get_values(self):
        return {'code': self.code, 'remote_identifier': self.remote_identifier}

class GossipMessage(Message):
    def __init__(self, data):
        super().__init__(MESSAGE_CODE_GOSSIP, data)

    def get_values(self):
        return {'code': self.code, 'message': self.data}

class RegistrationMessage(Message):
    def __init__(self, data):
        super().__init__(MESSAGE_CODE_REGISTRATION, data)
        code_hi, code_lo = struct.unpack('2B', data[0:2])
        self.regis_code = bytes_to_short(code_hi, code_lo)
        self.regis_iden = data[2:len(data)].decode()
    
    def get_values(self):
        return {'code': self.code, 'message': self.data, 'regis_code': self.regis_code, 'regis_iden': self.regis_iden}
        

MESSAGE_TYPES = {
    MESSAGE_CODE_GOSSIP: GossipMessage,
    MESSAGE_CODE_REGISTRATION: RegistrationMessage,
    MESSAGE_CODE_NEW_CONNECTION: NewConnectionMessage,
    MESSAGE_CODE_CONNECTION_LOST:ConnectionLostMessage
}