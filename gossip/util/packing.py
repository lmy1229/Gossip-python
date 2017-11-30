import logging
import struct
import socket

from gossip.util.message_codes import *

def short_to_bytes(short):
    return bytes([(int(short) >> 8) & 0xff]) + bytes([int(short) & 0xff])

def bytes_to_short(hi, lo):
    return hi << 8 | lo

def pack_msg_registration(code, identifier):
    b_code = short_to_bytes(code)
    b_iden = bytes(identifier, 'ascii')
    return {'code': MESSAGE_CODE_REGISTRATION, 'data': b_code + b_iden}

def send_msg(sock, code, msg):

    size = len(msg)
    b_size = short_to_bytes(size)
    b_code = short_to_bytes(code)
    b_msg = b_size + b_code + msg
    logging.info('Send Message: %d bytes, #%d, %s' % (size, code, b_msg))
    sock.send(b_msg)

def recv_msg(sock):
    
    header = sock.recv(4)
    if len(header) == 0:
        raise Exception('Client disconnected!')
    elif len(header) < 4:
        raise Exception('Incorrect header format!')

    size_hi, size_lo, code_hi, code_lo = struct.unpack('4B', header)
    size = bytes_to_short(size_hi, size_lo)
    code = bytes_to_short(code_hi, code_lo)

    data = sock.recv(size)
    if len(data) < size:
        raise Exception('Incomplete data!')

    logging.info('Received Message: %d bytes, #%d, %s' % (size, code, data))
    return {'size': size, 'code': code, 'data': data}
