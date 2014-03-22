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
    refactor especially the way types are handled and the get_data method and the serializing methods 
    need some refactoring of function names
    need some docstrings
    add in async/concurrency stuff for speed
    profile!
    integrate back into connection class and do full tests    
    clarify handling of OrderedDict
    add in pd.Series
    clarify handling of sorted and keyed tables
    add indicies (associated with keys)
'''
import itertools
import pandas
import datetime
import numpy as np
from bitstring import BitStream, pack
from collections import OrderedDict

#types: -ve is atomic +ve is vector
types = {
        -1: ('int', '4'), #bool
        1: ('int', '4', np.bool), #bool vector
        -4: ('int','8'), #byte
        4: ('int','8', np.int8), #byte vector
        -5: ('int', '16'), #short
        5: ('int', '16', np.int16), #short vector
        -6: ('int','32'), #int
        6: ('int','32', np.int32), #int vector
        -7: ('int','64'), #long
        7: ('int','64', np.int64), #long vector
        -8: ('float','32'), #real
        8: ('float','32', np.float32), #real vector
        -9: ('float','64'), #float
        9: ('float','64', np.float64), #float vector
        -10:('int', '8'), #char
        10:('int', '8', np.char), #char vector
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

inv_types = {
        int: (-6, 'int', '32'),
        list: (0, '', ''),
        dict: (99, '', ''),
        str: (-11, 'hex', '8'),
        OrderedDict: (127, '', ''),
        np.int64: (6, 'int', '32'),
        np.int8: (4, 'int', '8'),
        np.object_: (11, 'hex', 8,),
        pandas.DataFrame: (98, '', ''),
        }
INT = -6
BYTE = -4
NULL = BitStream('0x00')
Y2KDAYS = datetime.datetime(2000,1,1).toordinal()
MILLIS = 8.64E7 

#header format
header_format = 'intle:8=endian, intle:8=async, pad:16, intle:32=length, bits=data'

def format_bits(data, endianness = 'le'):
    endian = 1 if endianness == 'le' else 0
    data_format = header_format
    data = parse_on_the_wire(data, endianness)
    length = len(data)/8 + 8
    objects = {'endian':endian, 'async':0, 'length': length, 'data':data}
    bstream = pack(data_format, **objects)
    return bstream

#This is looking like it needs a refactor!
def parse_on_the_wire(data, endianness, attributes = 0):
    if isinstance(data,np.ndarray):
        dtype = inv_types[data.dtype.type]
        if data.dtype.type == np.object_:
            data_format = 'int{0}:8=type, int{0}:8=attributes, int{0}:32=length, bits'.format(endianness)
            bstream = pack(data_format, sum([parse_on_the_wire(i, endianness) for i in data]), type = dtype[0], attributes=attributes, length=len(data))
        else:
            data_format = 'int{0}:8=type, int{0}:8=attributes, int{0}:32=length, {3}*{1}{0}:{2}'.format(endianness, dtype[1], dtype[2], len(data))
            bstream = pack(data_format, *data, type=dtype[0], attributes=attributes, length=len(data))
    elif isinstance(data, list):
        type_set = set([type(i) for i in data])
        if len(type_set) == 1 and not list(type_set)[0] == np.ndarray:
            dtype = inv_types[list(type_set)[0]]
            if list(type_set)[0] == str or list(type_set)[0] == list:
                data_format = 'int{0}:8=type, int{0}:8=attributes, int{0}:32=length, bits'.format(endianness)
                bstream = pack(data_format, sum([parse_on_the_wire(i, endianness) for i in data]), type = -dtype[0], attributes=attributes, length=len(data))
            else:
                data_format = 'int{0}:8=type, int{0}:8=attributes, int{0}:32=length, {3}*{1}{0}:{2}'.format(endianness, dtype[1], dtype[2], len(data))
                bstream = pack(data_format, *data, type=-dtype[0], attributes=attributes, length=len(data))
        else:
            dtype = inv_types[type(data)]
            data_format = 'int{0}:8=type, int{0}:8=attributes, int{0}:32=length, bits=data'.format(endianness)
            bits = sum([parse_on_the_wire(i, endianness) for i in data])
            bstream = pack(data_format, data=bits, type=-dtype[0], attributes=attributes, length=len(data))
    elif type(data) == dict:
        dtype = inv_types[type(data)]
        data_format = 'int{0}:8=type, bits=data'.format(endianness)
        keys = parse_on_the_wire(data.keys(), endianness)
        vals = parse_on_the_wire(data.values(), endianness)
        bits = keys + vals
        bstream = pack(data_format, data=bits, type=dtype[0], attributes=attributes, length=len(data))
    elif type(data) == OrderedDict:
        dtype = inv_types[type(data)]
        data_format = 'int{0}:8=type, bits=data'.format(endianness)
        keys = parse_on_the_wire(data.keys(), endianness, 1)
        vals = parse_on_the_wire(data.values(), endianness)
        bits = keys + vals
        bstream = pack(data_format, data=bits, type=dtype[0], attributes=attributes, length=len(data))
    elif isinstance(data, str):
        bstream = pack('{0}*hex:8'.format(len(data)),*[hex(ord(i)) for i in data]) + BitStream(b'0x00')
    elif type(data) == pandas.DataFrame:
        is_sorted = 1 if data.index.is_monotonic else 0
        dtype = inv_types[type(data)]
        data_format = 'int{0}:8=type, int{0}:8=tabattrib, int{0}:8=dicttype, bits=cols,int{0}:8=typearray, int{0}:8=attributes, int{0}:32=length, bits=vals'.format(endianness)
        cols = parse_on_the_wire(data.columns.values, endianness)
        vals = sum(parse_on_the_wire(col.values, endianness) for i,col in data.iterkv())
        bstream = pack(data_format, cols=cols, type=dtype[0], typearray=0, attributes=0, length=len(data.columns), vals=vals, tabattrib=is_sorted, dicttype=99)
        
    else:    
        dtype = inv_types[type(data)]
        data_format = 'int{0}:8=type, {1}{0}:{2}'.format(endianness, dtype[1], dtype[2])
        bstream = pack(data_format, data, type=dtype[0])
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

#currently going through a rather large refit of this method
#the goal is to make some simple/clean/testable code
#need to reverse this next to make data go onto the wire!
#need to think of an efficient way to do that!
#also need to test this V for speed
def get_symbol(bstream, endianness, val_type):
    return str_convert(bstream, endianness)

def get_bool(bstream, endianness, val_type):
    data = -1
    while data == -1:
        data = [bool(x) for i, x  in enumerate(
            bstream.readlist(format_list(val_type, '', 2))) if i%2 == 1][0]
    return data    

def get_month(bstream, endianness, val_type):
    return get_date_from_q(bstream.read(format(val_type, endianness)))

def get_date(bstream, endianness, val_type):
    return datetime.datetime.fromordinal(bstream.read(format(val_type, endianness))+Y2KDAYS)

def get_datetime(bstream, endianness, val_type):
    dt = bstream.read(format(val_type, endianness))
    return datetime.datetime.fromordinal(int(dt)+Y2KDAYS) + datetime.timedelta(milliseconds = dt%1*MILLIS)
    
def get_bool_list(bstream, endianness, val_type):
    attributes = bstream.read(8).int
    length = bstream.read(format(INT, endianness))
    data = [bool(x) for i,x in enumerate(bstream.readlist(format_list(val_type, '', 2*length))) if i%2 == 1]
    return data

def get_symbol_list(bstream, endianness, val_type):
    attributes = bstream.read(8).int
    length = bstream.read(format(INT, endianness))
    data = [str_convert(bstream, endianness) for i in range(length)]
    return data

def get_char_list(bstream, endianness, val_type):
    attributes = bstream.read(8).int
    length = bstream.read(format(INT, endianness))
    nptype, bstype = format_raw_list(val_type, length)
    data = bstream.read(bstype).bytes
    return data

def get_month_list(bstream, endianness, val_type):
    attributes = bstream.read(8).int
    length = bstream.read(format(INT, endianness))
    data = [get_date_from_q(x) for x in bstream.readlist(format_list(val_type, endianness, length))]
    return data

def get_date_list(bstream, endianness, val_type):
    attributes = bstream.read(8).int
    length = bstream.read(format(INT, endianness))
    data = [datetime.datetime.fromordinal(x+Y2KDAYS) for x in bstream.readlist(format_list(val_type, endianness, length))]
    return data

def get_datetime_list(bstream, endianness, val_type):
    attributes = bstream.read(8).int
    length = bstream.read(format(INT, endianness))
    dt = bstream.readlist(format_list(val_type, endianness, length))
    data = [datetime.datetime.fromordinal(int(x)+Y2KDAYS)+datetime.timedelta(milliseconds=x%1*MILLIS) for x in dt]
    return data

def get_table(bstream, endianness, val_type):
    attributes = bstream.read(8).int
    data = pandas.DataFrame(get_data(bstream, endianness))
    return data

def get_dict(bstream, endianness, val_type):
    keys = get_data(bstream, endianness)
    vals = get_data(bstream, endianness)
    if isinstance(keys, pandas.DataFrame):
        data = pandas.concat([keys, vals], axis = 1)
    else:    
        data = dict(zip(keys, vals))
    return data

def get_lambda_func(bstream, endianness, val_type):
    context = str_convert(bstream, endianness)
    data = '.' + context + get_data(bstream, endianness)
    return data

def get_ordered_dict(bstream, endianness, val_type):
    keys = get_data(bstream, endianness)
    vals = get_data(bstream, endianness)
    if isinstance(keys, pandas.DataFrame):
        data = pandas.concat([keys, vals], axis = 1)
    else:    
        data = OrderedDict(zip(keys, vals))
    return data    

int_types = {-11:get_symbol,
    -1:get_bool,
    -13:get_month,
    -14:get_date,
    -15:get_datetime,
    -20:[],
    1:get_bool_list,
    10:get_char_list,
    11:get_symbol_list,
    13:get_month_list,
    14:get_date_list,
    15:get_datetime_list,
    20:[],
    98:get_table,
    99:get_dict,
    100:get_lambda_func,
    127:get_ordered_dict
    }

def format_raw_list(val_type, length):
    type_spec = types[val_type]
    return type_spec[2], length*int(type_spec[1])

def get_data(bstream, endianness):
    #import ipdb;ipdb.set_trace()
    val_type = bstream.read(8).int
    if val_type in int_types:
        data = int_types[val_type](bstream, endianness, val_type)
    elif -20 < val_type < -10:
        data = get_hour(bstream.read(format(val_type, endianness)))
    elif val_type < 0:
        data = bstream.read(format(val_type, endianness))
    elif 20 > val_type > 10:
        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        data = [get_hour(x) for x in bstream.readlist(format_list(val_type, endianness, length))]
    elif 10 > val_type > 0:
        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        nptype, bstype = format_raw_list(val_type, length)
        data = np.fromstring(bstream.read(bstype).bytes, dtype=nptype)
        #data = bstream.readlist(format_list(val_type, endianness, length))
    elif val_type > 90:
        data = []
    else:
        attributes = bstream.read(8).int
        length = bstream.read(format(INT, endianness))
        data = [get_data(bstream, endianness) for _ in range(length)]
    return data        

def parse(bits):
    bstream = BitStream(bits)
    endianness, size = get_header(bstream)
    while (bstream.pos < 8*size):
        data = get_data(bstream, endianness)
    return data    


