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
    more test cases for other types
    fill in types map
    optimize dict handling?
    have to reverse this to generate bit stream for putting onto socket
    refactor especially the way types are handled and the get_data method
    add in async/concurrency stuff for speed
    profile!
    integrate back into connection class and do full tests    
'''
import itertools
import pandas
from bitstring import BitStream
from collections import OrderedDict

types = {
        -1: ('int', '4'), #bool
        1: ('int', '4'), #bool vector
        -4: ('int','8'), #byte
        4: ('int','8'), #byte vector
        -5: ('int', '16'), #short
        5: ('int', '16'), #short vector
        -6: ('int','32'), #int
        6: ('int','32'), #int vector
        -7: ('int','64'), #long
        7: ('int','64'), #long vector
        -10:('int', '8'), #char
        10:('int', '8'), #char vector
        -11:('symbol',''), #symbol
        11:('symbol',''), #symbol vector
         0:('list','0'), #list
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
    elif val_type == -1:
        data = -1
        while data == -1:
            data = [bool(x) for i,x  in enumerate(bstream.readlist(format_list(val_type, '', 2))) if i%2 == 1][0]
    elif val_type < 0:
        data = bstream.read(format(val_type, endianness))
    elif val_type == 11:    
        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        data = [str_convert(bstream, endianness) for i in range(length)]
    elif val_type == 1:
        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        data = [bool(x) for i,x in enumerate(bstream.readlist(format_list(val_type, '', 2*length))) if i%2 == 1]
    elif 90 > val_type > 0:
        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        data = bstream.readlist(format_list(val_type, endianness, length))
    elif val_type == 99:
        #import ipdb;ipdb.set_trace()
        keys = get_data(bstream, endianness)
        vals = get_data(bstream, endianness)
        if isinstance(keys, pandas.DataFrame):
            data = pandas.concat([keys, vals], axis = 1)
        else:    
            data = dict(zip(keys, vals))
    elif val_type == 98:
        attributes = bstream.read(8).int
        data = pandas.DataFrame(get_data(bstream, endianness))
    elif val_type == 127:
        keys = get_data(bstream, endianness)
        vals = get_data(bstream, endianness)
        if isinstance(keys, pandas.DataFrame):
            data = pandas.concat([keys, vals], axis = 1)
        else:    
            data = OrderedDict(zip(keys, vals))
    elif val_type == 100:
        context = str_convert(bstream, endianness)
        data = '.' + context + ''.join([chr(i) for i in get_data(bstream, endianness)])
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

def test_table_simple():
    data = pandas.DataFrame([{'a':2,'b':3}])
    bits = b'0x010000002f0000006200630b0002000000610062000000020000000600010000000200000006000100000003000000'
    assert (data.values == parse(bits).values).all()
    
def test_table_ordered():
    data = pandas.DataFrame([{'a':2,'b':3}])
    bits = b'0x010000002f0000006201630b0002000000610062000000020000000603010000000200000006000100000003000000'
    assert (data.values == parse(bits).values).all()
    
def test_keyed_table():
    data = pandas.DataFrame([{'a':2,'b':3}])
    bits = b'0x010000003f000000636200630b00010000006100000001000000060001000000020000006200630b0001000000620000000100000006000100000003000000'
    assert (data.values == parse(bits).values).all()

def test_sorted_keyed_table():
    data = pandas.DataFrame([{'a':2,'b':3}])
    bits = b'0x010000003f0000007f6201630b00010000006100000001000000060001000000020000006200630b0001000000620000000100000006000100000003000000'
    assert (data.values == parse(bits).values).all()

def test_function():
    data = '.{x+y}'
    bits = b'0x010000001500000064000a00050000007b782b797d'
    assert data == parse(bits)
    
def test_non_root_function():
    data = '.d{x+y}'
    bits = b'0x01000000160000006464000a00050000007b782b797d'
    assert data == parse(bits)
    
def test_bool():
    data = False
    bits = b'0x010000000a000000ff00'
    assert data == parse(bits)
    
def test_bool_vector():
    data = [False]
    bits = b'0x010000000f00000001000100000000'
    assert data == parse(bits)
    
def test_short():
    data = 1
    bits = b'0x010000000b000000fb0100'
    assert data == parse(bits)

def test_short_vector():
    data = [1]
    bits = b'0x01000000100000000500010000000100'
    assert data == parse(bits)
    
def test_long():
    data = 1
    bits = b'0x0100000011000000f90100000000000000'
    assert data == parse(bits)

def test_long_vector():
    data = [1]
    bits = b'0x01000000160000000700010000000100000000000000'
    assert data == parse(bits)

x = '''
have to test:
    real 8
    float 9
    month 13
    date 14
    datetime 15
    minute 17
    second 18
    time 19
    ??enum 20
'''    

