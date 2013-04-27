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
'''
from bitstring import BitStream

types = {
        6: ('int','32'), #int vector
        -6: ('int','32'), #int
        4: ('int','8'), #byte vector
        -4: ('int','8'), #byte
        0:('list','0'), #list
        
        }

INT = -6

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
    if val_type < 0:
        data = bstream.read(format(val_type, endianness))
    elif 90 > val_type > 0:
        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        data = bstream.readlist(format_list(val_type, endianness, length))
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
    

