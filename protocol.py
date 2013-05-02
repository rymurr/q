'''
Primary source of kdb ipc protocol definitions
here we define all the q data types and their on the wire form
A parser is used to convert between the python format and kdb/q format

types are found here:
    http://www.kx.com/q/d/q1.htm

Note on dates and times
    dates are number of days since Jan 1 2000
    times are number of hours/minutes/seconds/millis
    datetimes are float64 days since Jan 1 (fractional day is converted to millis and parsed)

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
from bitstring import BitStream, pack
from collections import OrderedDict

#header format
header_format = 'intle:8=endian, intle:8=async, pad:16, intle:32=length'

#types: -ve is atomic +ve is vector
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

def parse_on_the_wire(data):
    data_format = header_format
    objects = {'endian':1, 'async':0}
    if isinstance(data,int):
        data_format += ',intle:8=type, intle:32=data'
        objects['type'] = -6
        objects['data'] = data
        objects['length'] = 8 + 5
        
    bstream = pack(data_format, **objects)
    return bstream
   

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


