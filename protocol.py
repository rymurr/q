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

TODO:
    factor out endian convert stuff
    get rid of if statement somehow?
'''
from bitstring import BitStream

types = {
        6: ('int','32'), #int vector
        -6: ('int','32'), #int
        4: ('int','8'), #byte vector
        -4: ('int','8'), #byte
        
        }

INT = -6

def format(val_type, endianness):
    type_spec = types[val_type]
    return type_spec[0]+endianness+':'+type_spec[1]

def format_list(val_type, endianness, length):
    type_spec = types[val_type]
    return str(length)+'*'+type_spec[0]+endianness+':'+type_spec[1]

def parse(bits):
    bstream = BitStream(bits)
    endian = bstream.read(8).int
    msg_type = bstream.read(8).int
    _ = bstream.read(16)
    endianness = 'le' if endian == 1 else 'be'
    size = bstream.read(format(INT, endianness))
    while (bstream.pos < 8*size):
        val_type = bstream.read(8).int
        if val_type < 0:
            data = bstream.read(format(val_type, endianness))
        else:
            attributes = bstream.read(8).int
            length = bstream.read(format(INT, endianness))
            data = bstream.readlist(format_list(val_type, endianness, length))

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
