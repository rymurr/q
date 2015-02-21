from gevent import monkey; monkey.patch_all()
import gevent
import socket
import array
import time
import cStringIO
import bitstring
from q.unparser import format_bits
from q.utils import get_header
from q.parser import parse

def foo():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost',5010))
    login = array.array('b','' + '\x03\x00') #null terminated signed char array (bytes)
    sock.send(login.tostring())
    result = sock.recv(1)  #blocking recv
    sock.send(format_bits('.u.sub[`trade;`]', async=True, symbol=False, endianness='be').tobytes())

    while True:
        data=cStringIO.StringIO()
        header = sock.recv(8)
        data.write(header)
        data.reset()
        _,size = get_header(bitstring.ConstBitStream(bytes=data.read()))
        print size
        while True:
            data.write(sock.recv(size))
            if data.tell() < size:
                continue
            else:
                break
        data.reset()
        xxx = bitstring.ConstBitStream(bytes=data.read())    
        yield parse(xxx)[-1]

if __name__ == '__main__':
    for i in foo():
        print i
   
 
