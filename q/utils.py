import datetime
import itertools 
from protocol import types, inv_types, header_format, MILLIS, Y2KDAYS, NULL, BYTE, INT

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
    bstream.read(16)
    endianness = 'le' if endian == 1 else 'be'
    size = bstream.read(format(INT, endianness))
    return endianness, size

def get_date_from_q(i):
    m = i + 24000
    year = m/12
    month = m % 12+1
    day = 1
    return datetime.datetime(year, month, day)

def get_hour(i):
    if i/3600000 > 0:
        hour = (i/1000)/3600
        minute = ((i/1000)/60) % 60
        second = (i/1000) % 60
        micro = i % 1000
    elif i/3600 > 0:
        hour = i/3600
        minute = (i/60) % 60
        second = i % 60
        micro = 0
    else:
        hour = i/60
        minute = i % 60
        second = 0
        micro = 0
    return datetime.time(hour, minute, second, micro)

def format_raw_list(val_type, length):
    type_spec = types[val_type]
    return type_spec[2], length*int(type_spec[1])


