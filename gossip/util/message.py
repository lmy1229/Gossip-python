from gossip.util.packing import short_to_bytes

class Message():
    """ basic class of message """
    def __init__(self, code, data):
        self.code = code
        self.data = data

    def encode(self):
        size = len(self.data)
        b_size = short_to_bytes(size)
        b_code = short_to_bytes(self.code)
        b_msg = b_size + b_code + self.datas
        return b_msg
        

MESSAGE_TYPES = {}