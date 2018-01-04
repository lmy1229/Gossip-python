import json
import mmh3
import logging
import sys
import socket

from prompt_toolkit import prompt
from prompt_toolkit.contrib.completers import WordCompleter
from prompt_toolkit.validation import Validator, ValidationError
from prompt_toolkit.history import InMemoryHistory

from cassandra.util.message import RequestMessage, ResponseMessage
from cassandra.util.packing import recv_msg, addr_tuple_to_str

# class CassandraClientGrammarValidator(Validator):
#     """Validator for client input"""
#     def validate(self, document):
        
#         text = document.text
#         if 

class CassandraClient():
    """
    Client for Naive Cassandra

        connection_status:
            0 : not connected
            1 : connected

    """
    def __init__(self, **kwargs):

        self.connection_status = 0
        self.connection_addr = ''
        self.addr = ''
        self.socket = None

        self.commands = ['put', 'get', 'connect', 'disconnect', 'exit', 'set']

    def start(self):
        
        completer = WordCompleter(self.commands)
        history = InMemoryHistory()

        handlers = {
            'put': self.handle_put,
            'get': self.handle_get,
            'connect': self.handle_connect,
            'disconnect': self.handle_disconnect,
            'exit': self.handle_exit,
            'set': self.handle_set
        }

        while True:
            text = prompt('>> ', history=history, completer=completer)
            # parse command
            cmd = None
            try:
                cmd = self.parseInput(text)
            except ValidationError as e:
                print(e)
                self.printUsage()
                continue
            # handle command
            if cmd:
                handlers[cmd[0]](cmd)
        
    ################### PARSERS ######################
    '''
    CMD     = CONN || DISCONN || EXIT || GET || PUT
    Conn    = connect ADDR
    ADDR    = INT8.INT8.INT8.INT8:INT16
    DISCONN = disconnect
    EXIT    = exit
    GET     = get(Key)
    PUT     = put(Key,VALUES)
    VALUES  = Column=Value || Column=Value,VALUS
    '''

    def printUsage(self):
        pass

    def parseInput(self, document):
        parts = document.strip().split(' ')
        if len(parts) == 0:
            return None
        cmdType = parts[0]

        if cmdType not in self.commands:
            raise ValidationError(message='Invalid command type')

        if cmdType == 'disconnect':
            if not len(parts) == 1:
                raise ValidationError(message='Invalid grammar for %s' % cmdType)
            return ['disconnect']
        elif cmdType == 'connect':
            pass
        elif cmdType == 'exit':
            if not len(parts) == 1:
                raise ValidationError(message='Invalid grammar for %s' % cmdType)
            return ['exit'] 
        elif cmdType == 'put':
            pass
        elif cmdType == 'get':
            pass
        elif cmdType == 'set':
            pass


    ################## HANDLERS ######################

    def handle_connect(self, cmd):
        if not self.check_connect(True):
            print('already connected to %s' % self.connection_addr)
            return

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        saddr = cmd[1]
        addr, port = saddr.split(':')
        port = int(port)
        try:

            self.socket.connect((addr, port))
            print('connect to %s succeeded' % saddr)
            self.connection_status = 1
            self.connection_addr = saddr
            self.addr = addr_tuple_to_str(self.socket.getsockname())
        except Exception as e:
            print(e)

    def handle_disconnect(self, cmd):
        if not self.check_connect(False):
            print('not connected')
            return

        self.socket.close()
        print('disconnect from %s' % self.connection_addr)

        self.socket = None
        self.connection_addr = ''
        self.connection_status = 0

    def handle_exit(self, cmd):
        if self.socket:
            self.socket.close()
            self.socket = None
        sys.exit(0)

    def handle_put(self, cmd):
        if not self.check_connect(True):
            logging.error('Client | socket not connected')
            return
        key = cmd[1]
        value = cmd[2]
        send_request(['put', key, value])
        msg = get_response(self.socket)
        print(msg.description)

    def handle_get(self, cmd):
        if not self.check_connect(True):
            logging.error('Client | socket not connected')
            return
        key = cmd[1]
        send_request(['get', key])
        msg = get_response(self.socket)
        print(msg.description)

    def handle_set(self, cmd):
        pass

    def send_request(self, requeset):
        request_hash = mmh3.hash(json.dumps(self.addr, requeset))
        d = {'request': request, 'request_hash': request_hash}
        s = bytes(json.dumps(d), 'ascii')
        message = RequestMessage(s, self.addr)
        try:
            self.socket.send(message.encode())
        except Exception as e:
            print(e)
            raise e

    def get_response(self):
        msg = recv_msg(self.socket)
        return msg

    def check_connect(self, connected=False):
        if connected and not self.socket:
            return False
        if not connected and self.socket:
            return False
        return True


