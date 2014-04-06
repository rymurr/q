'''
TODO sort out send and recieve in cursor function! Lots of mess there
'''
import socket
import array
import cStringIO

from parser import parse
from unparser import format_bits
from utils import get_header


def connect(host = 'localhost', port = 5000, user = '', password = ''):
    '''
    return new q connection
    '''
    return Connection(host, port, user, password)

class Connection(object):
  '''
  connection class for q wrapper
  TODO make it look/act like DB-API 2.0 compliant db interface
  see http://www.python.org/dev/peps/pep-0249/
  '''

  def __init__(self, host='localhost', port=5000, user='', password = ''):
    self.host=host
    self.port=port
    self.user=user + ':' + password
    self.sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.connect()
      
  def close(self):
    '''
    close socket and end connection
    '''
    self.sock.close()
      
  def connect(self):
    '''
    make connection or throw an error
    '''
    try:
      self.sock.connect((self.host,self.port))
      login = array.array('b',self.user + '\0')  #null terminated signed char array (bytes)
      self.sock.send(login.tostring())
      result = self.sock.recv(1)  #blocking recv
      if not result:
        raise Exception("access denied")
    except:
      raise Exception ('unable to connect to host')
      
  def cursor(self):
      return Cursor(self.sock)


class Cursor(object):
  '''
  DB-API 2.0 compliant cursor obj
  '''
  def __init__(self, sock):
      self.sock = sock
      
  def execute(self, query):
    self._send(query)
    return self._receive()
      
  def _send(self, query):
    message = format_bits(query)
    self.last_outgoing=message
    print message
    print query
    self.sock.send(message.hex)

  def _receive(self):
    """read the response from the server"""
    header = self.sock.recv(8)
    endianness, size = get_header(BitStream(header))
    
    print header
    bytes = self._recv_size(data_size - 8)
    print bytes
    val = parse(BitStream(header+bytes))
    print val
    return val
  
  def _recv_size(self, size):
    """read size bytes from the socket."""
    data=cStringIO.StringIO()
    recv_size=min(size,8192)
    while data.tell()<size:
      data.write(self.sock.recv(recv_size))
    v = data.getvalue()
    data.close()
    return v
  
