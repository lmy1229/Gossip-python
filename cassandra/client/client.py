import json
import mmh3
import logging
import sys
import socket
import re

from prompt_toolkit import prompt
from prompt_toolkit.contrib.completers import WordCompleter
from prompt_toolkit.validation import Validator, ValidationError
from prompt_toolkit.history import InMemoryHistory, FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory

from cassandra.util.message import RequestMessage, ResponseMessage
from cassandra.util.message_codes import MESSAGE_CODE_RESPONSE
from cassandra.util.packing import recv_msg, addr_tuple_to_str, addr_str_to_tuple

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

        self.commands = ['put', 'get', 'connect', 'disconnect', 'exit', 'set', 'batchput', 'batchget']

    def start(self):
        
        completer = WordCompleter(self.commands)
        # history = InMemoryHistory()
        history = FileHistory('.client_history')

        handlers = {
            'put': self.handle_put,
            'get': self.handle_get,
            'connect': self.handle_connect,
            'disconnect': self.handle_disconnect,
            'exit': self.handle_exit,
            'set': self.handle_set,
            'batchput': self.handle_batchput,
            'batchget': self.handle_batchget
        }

        while True:
            text = prompt('>> ', history=history, completer=completer, auto_suggest=AutoSuggestFromHistory(), enable_history_search=True)
            # parse command
            cmd = None
            try:
                cmd = self.parseInput(text)
            except ValidationError as e:
                print(e)
                self.printUsage()
                continue
            # handle command
            try:
                if cmd:
                    handlers[cmd[0]](cmd)
            except Exception as e:
                print(e)

        
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
            if not len(parts) == 2:
                raise ValidationError(message='Invalid grammar for %s' % cmdType)
            return ['connect', self.parseAddr(parts[1])]
        elif cmdType == 'exit':
            if not len(parts) == 1:
                raise ValidationError(message='Invalid grammar for %s' % cmdType)
            return ['exit'] 
        elif cmdType == 'put':
            if not (len(parts) >= 3):
                raise ValidationError(message='Invalid grammar for %s' % cmdType)
            return ['put', parts[1], ' '.join(parts[2:])]
        elif cmdType == 'get':
            if not (len(parts) == 2):
                raise ValidationError(message='Invalid grammar for %s' % cmdType)
            return ['get', parts[1]]
        elif cmdType == 'set':
            pass
        elif cmdType == 'batchput':
            if not (len(parts) == 2):
                raise ValidationError(message='Invalid grammar for %s' % cmdType)
            return ['batchput', parts[1]]
        elif cmdType == 'batchget':
            if not (len(parts) == 2):
                raise ValidationError(message='Invalid grammar for %s' % cmdType)
            return ['batchget', parts[1]]

    def parseAddr(self, document):
        if not re.match(r'^\d+\.\d+\.\d+\.\d+:\d+$', document):
            raise ValidationError(message='Invalid Address')
        ip, port = document.split(':')
        for part in ip.split('.'):
            if int(part) < 0 or int(part) > 255:
                raise ValidationError(message='Invalid Address')
        if int(port) < 0 or int(port) > 65535:
            raise ValidationError(message='Invalid Address')

        return document


    ################## HANDLERS ######################

    def handle_connect(self, cmd):
        if not self.check_connect(False):
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
            self.addr = self.socket.getsockname()
        except Exception as e:
            print(e)

    def handle_disconnect(self, cmd):
        if not self.check_connect(True):
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

    def handle_put(self, cmd, file=sys.stdout):
        if not self.check_connect(True):
            logging.error('Client | socket not connected')
            return
        key = cmd[1]
        value = cmd[2]
        self.send_request(['put', key, value])
        msg = self.get_response()
        print(msg.description, file=file)

    def handle_get(self, cmd, file=sys.stdout):
        if not self.check_connect(True):
            logging.error('Client | socket not connected')
            return
        key = cmd[1]
        self.send_request(['get', key])
        msg = self.get_response()
        print(msg.description, file=file)

    def handle_set(self, cmd):
        pass

    def handle_batchput(self, cmd):
        filepath = cmd[1]
        outputpath = ('.output_put.log')
        with open(filepath, 'r') as file:
            with open(outputpath, 'a') as outfile:
                for l in file:
                    if len(l) == 0:
                        return
                    key = l.strip().split(',')[0]
                    value = l.strip()
                    self.handle_put(['put', key, value], file=outfile)


    def handle_batchget(self, cmd):
        filepath = cmd[1]
        outputpath = ('.output_get.log')
        with open(filepath, 'r') as file:
            with open(outputpath, 'a') as outfile:
                for l in file:
                    if len(l) == 0:
                        return
                    key = l.strip()
                    self.handle_get(['get', key], file=outfile)

    def send_request(self, request):
        request_hash = mmh3.hash(json.dumps((self.addr, tuple(request))))
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
        if not msg['code'] == MESSAGE_CODE_RESPONSE:
            raise Exception('Message Code Error')

        return ResponseMessage(msg['data'], msg['source'])

    def check_connect(self, connected=False):
        if connected and not self.socket:
            return False
        if not connected and self.socket:
            return False
        return True


