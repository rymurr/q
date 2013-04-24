'''
first 8 bits = endianness: 01 == little, 00 == big
next 8 bits = message type: 0=async, 1=sync, 2=response
next 16 bits = blank
next 32 bits = size of message (total size!)
REST IS DATA!
next 8 bits = type of element..convert to int. Map of types to follow
then data
'''
from bitstring import BitStream

def parse(bits):
    bstream = BitStream(bits)
    endian = bstream.read(8).int
    msg_type = bstream.read(8).int
    _ = bstream.read(16)
    size_raw = bstream.read(32)
    endian_convert = lambda x: getattr(x, 'intle' if endian == 1 else 'intbe')
    endian_convert_list = '*intle:32' if endian == 1 else '*intbe:32'
    size = endian_convert(size_raw)
    while (bstream.pos < 8*size):
        val_type = endian_convert(bstream.read(8))
        if val_type == -6:
            data = endian_convert(bstream.read(32))
        elif val_type == 6:
            attributes = bstream.read(8)
            length = endian_convert(bstream.read(32))
            data = bstream.readlist(str(length)+endian_convert_list)
            
    return data        

def test_int():
    data = 1
    bits = b'0x010000000d000000fa01000000'
    assert data == parse(bits)
    

def test_int_vector():
    data = [1]
    bits = b'0x010000001200000006000100000001000000'
    assert data == parse(bits)
