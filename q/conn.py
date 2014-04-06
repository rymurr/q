'''
TODO sort out send and recieve in cursor function! Lots of mess there
'''
import socket
import array
import cStringIO

import parse 


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
      self.parser = parse.Parser()
      self.parser.update_types()
      
  def execute(self, query):
    self._send(query)
    return self._receive()
      
  def _send(self, query):
    message = array.array('b', [0,1,0,0]) # 1 for synchronous requests
    message.fromstring(self.parser.write_integer(0)) # reserve space for message length
    message = self.parser.write(query,message)
    message[4:8] = self.parser.write_integer(len(message))
    print message
    self.last_outgoing=message
    self.sock.send(message)

  def _receive(self):
    """read the response from the server"""
    header = self.sock.recv(8)
    #Endianness of byte doesn't matter when determining endianness
    endianness = lambda x:x
    if not self.parser.read_byte(endianness,0,header)[0] == 1:
      endianness = '>'.__add__
    (data_size,self.offset) = self.parser.read_integer(endianness,4,header)
    
    bytes = self._recv_size(data_size - 8)
    #ensure that it reads all the data
    if self.parser.read_byte(endianness,0,bytes)[0] == -128 :
      (val,self.offset) = self.parser.read_symbol(endianness,1,bytes)
      raise Exception(val)
    (val,self.offset) = self.parser.read(endianness,0,bytes)
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
  
