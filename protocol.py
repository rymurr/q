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
    missing enum!
    have to reverse this to generate bit stream for putting onto socket
    refactor especially the way types are handled and the get_data method
    add in async/concurrency stuff for speed
    profile!
    integrate back into connection class and do full tests    
'''
import itertools
import pandas
import datetime
from bitstring import BitStream
from collections import OrderedDict

header_format = 'int:8=endian, int:8=async, pad:16, int:32=length'
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
        -8: ('float','32'), #real
        8: ('float','32'), #real vector
        -9: ('float','64'), #float
        9: ('float','64'), #float vector
        -10:('int', '8'), #char
        10:('int', '8'), #char vector
        -11:('symbol',''), #symbol
        11:('symbol',''), #symbol vector
        -13:('int', '32'), #month
        13:('int', '32'), #month vector
        -14:('int', '32'), #date
        14:('int', '32'), #date vector
        -15:('float', '64'), #datetime
        15:('float', '64'), #datetime vector
        -17:('int', '32'), #hour
        17:('int', '32'), #hour vector
        -18:('int', '32'), #second
        18:('int', '32'), #second vector
        -19:('int', '32'), #time
        19:('int', '32'), #time vector
         0:('list','0'), #list
        }

INT = -6
BYTE = -4
Y2KDAYS = datetime.datetime(2000,1,1).toordinal()
MILLIS = 8.64E7 

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

def get_date(i):
    m = i + 24000
    year = m/12
    month = m%12+1
    day = 1
    return datetime.datetime(year, month, day)

def get_hour(i):
    if i/3600000 > 0:
        hour = (i/1000)/3600
        minute = ((i/1000)/60)%60
        second = (i/1000)%60
        micro = i%1000
    elif i/3600 > 0:
        hour = i/3600
        minute = (i/60)%60
        second = i%60
        micro = 0
    else:
        hour = i/60
        minute = i%60
        second = 0
        micro = 0
    return datetime.time(hour, minute, second, micro)

def get_data(bstream, endianness):
    val_type = bstream.read(8).int
    if val_type == -11:
        data = str_convert(bstream, endianness)
    elif val_type == -1:
        data = -1
        while data == -1:
            data = [bool(x) for i,x  in enumerate(bstream.readlist(format_list(val_type, '', 2))) if i%2 == 1][0]
    elif val_type == -13:
        data = get_date(bstream.read(format(val_type, endianness)))
    elif val_type == -14:
        data = datetime.datetime.fromordinal(bstream.read(format(val_type, endianness))+Y2KDAYS)
    elif val_type == -15:
        dt = bstream.read(format(val_type, endianness))
        data = datetime.datetime.fromordinal(int(dt)+Y2KDAYS) + datetime.timedelta(milliseconds = dt%1*MILLIS)
    elif val_type == -20:
        data = []
    elif -20 < val_type < -10:
        data = get_hour(bstream.read(format(val_type, endianness)))
    elif val_type < 0:
        data = bstream.read(format(val_type, endianness))
    elif val_type == 1:
        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        data = [bool(x) for i,x in enumerate(bstream.readlist(format_list(val_type, '', 2*length))) if i%2 == 1]
    elif val_type == 11:    
        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        data = [str_convert(bstream, endianness) for i in range(length)]
    elif val_type == 13:
        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        data = [get_date(x) for x in bstream.readlist(format_list(val_type, endianness, length))]
    elif val_type == 14:
        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        data = [datetime.datetime.fromordinal(x+Y2KDAYS) for x in bstream.readlist(format_list(val_type, endianness, length))]
    elif val_type == 15:
        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        dt = bstream.readlist(format_list(val_type, endianness, length))
        data = [datetime.datetime.fromordinal(int(x)+Y2KDAYS)+datetime.timedelta(milliseconds=x%1*MILLIS) for x in dt]
    elif val_type == 20:
        data = []
    elif 90 > val_type > 10:
        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        data = [get_hour(x) for x in bstream.readlist(format_list(val_type, endianness, length))]
    elif 10 >= val_type > 0:
        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        data = bstream.readlist(format_list(val_type, endianness, length))
    elif val_type == 99:
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


