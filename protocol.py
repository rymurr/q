'''
first 8 bits = endianness: 01 == little, 00 == big
next 8 bits = message type: 0=async, 1=sync, 2=response
next 16 bits = blank
next 32 bits = size of message (total size!)
REST IS DATA!
next 8 bits = type of element..convert to int. Map of types to follow
then data


-ve is atomic type
+ve is vector of atomic type

tests come from examples given here:
    http://code.kx.com/wiki/Reference/ipcprotocol
types are found here:
    http://www.kx.com/q/d/q1.htm

TODO:
    make able to do recursive for more complex structures
    think a bit about speed
    more test cases
    fill in types map
    optimize dict handling?
'''
import itertools
from bitstring import BitStream
from collections import OrderedDict

types = {
        6: ('int','32'), #int vector
        -6: ('int','32'), #int
        4: ('int','8'), #byte vector
        -4: ('int','8'), #byte
        0:('list','0'), #list
        -11:('symbol',''), #symbol
        11:('symbol',''), #symbol vector
        }

INT = -6
BYTE = -4

class iter_char(object):
    def __init__(self, bstream, endianness):
        self.bstream = bstream
        self.endianness = endianness
    def __iter__(self):
        while self.bstream.pos < self.bstream.len:
            x = self.bstream.read(format(BYTE,self.endianness))
            yield x

def str_convert(bstream, endianness):
    return ''.join([chr(i) for i in itertools.takewhile(lambda x: x!=0, iter_char(bstream, endianness))])
    
def format(val_type, endianness):
    type_spec = types[val_type]
    return type_spec[0]+endianness+':'+type_spec[1]

def format_list(val_type, endianness, length):
    type_spec = types[val_type]
    return str(length)+'*'+type_spec[0]+endianness+':'+type_spec[1]

def get_header(bstream):
    endian = bstream.read(8).int
    msg_type = bstream.read(8).int
    _ = bstream.read(16)
    endianness = 'le' if endian == 1 else 'be'
    size = bstream.read(format(INT, endianness))
    return endianness, size

def get_data(bstream, endianness):
    val_type = bstream.read(8).int
    if val_type == -11:
        data = str_convert(bstream, endianness)
    elif val_type < 0:
        data = bstream.read(format(val_type, endianness))
    elif val_type == 11:    
        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        data = [str_convert(bstream, endianness) for i in range(length)]
    elif 90 > val_type > 0:

        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        data = bstream.readlist(format_list(val_type, endianness, length))
    elif val_type == 99:
        keys = get_data(bstream, endianness)
        vals = get_data(bstream, endianness)
        data = dict(zip(keys, vals))
    elif val_type == 127:
        keys = get_data(bstream, endianness)
        vals = get_data(bstream, endianness)
        data = OrderedDict(zip(keys, vals))
    elif val_type > 90:
        data = []
    else:
        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        data = [get_data(bstream, endianness) for i in range(length)]

    return data        

def parse(bits):
    bstream = BitStream(bits)
    endianness, size = get_header(bstream)
    while (bstream.pos < 8*size):
        data = get_data(bstream, endianness)
    return data    

def test_int():
    data = 1
    bits = b'0x010000000d000000fa01000000'
    assert data == parse(bits)

def test_int_vector():
    data = [1]
    bits = b'0x010000001200000006000100000001000000'
    assert data == parse(bits)

def test_byte_vector():
     data = [0,1,2,3,4]
     bits = b'0x01000000130000000400050000000001020304'
     assert data == parse(bits)
     
def test_list():
    data = [[0,1,2,3,4]]
    bits = b'0x01000000190000000000010000000400050000000001020304'
    assert data == parse(bits)
    
def test_simple_dict():
    data = {'a':2,'b':3}
    bits = b'0x0100000021000000630b0002000000610062000600020000000200000003000000'
    assert data == parse(bits) 
    
def test_ordered_dict():
    data = {'a':2,'b':3}
    bits = b'0x01000000210000007f0b0102000000610062000600020000000200000003000000'
    assert data == parse(bits) 

def test_dict_vector():
    data = {'a':[2], 'b':[3]}
    bits = b'0x010000002d000000630b0002000000610062000000020000000600010000000200000006000100000003000000'
    assert data == parse(bits)
