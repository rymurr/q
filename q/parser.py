import pandas
import numpy as np
import datetime
from bitstring import ConstBitStream
from protocol import types, inv_types, header_format, MILLIS, Y2KDAYS, NULL, BYTE, INT, Y2KMILLIS
from utils import str_convert, format, format_list, get_header, get_date_from_q, get_hour, format_raw_list 
from collections import OrderedDict

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
    
def get_nanodatetime(bstream, endianness, val_type):
    dt = bstream.read(format(val_type, endianness))/1E9
    return datetime.datetime.utcfromtimestamp(dt+Y2KMILLIS) 
    
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

def get_nanodatetime_list(bstream, endianness, val_type):
    attributes = bstream.read(8).int
    length = bstream.read(format(INT, endianness))
    dt = bstream.readlist(format_list(val_type, endianness, length))
    data = [datetime.datetime.utcfromtimestamp(x/1E9+Y2KMILLIS) for x in dt]
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
        data = data.set_index(list(keys.columns))
    else:    
        data = dict(zip(keys, vals))
    return data

def get_lambda_func(bstream, endianness, val_type):
    context = str_convert(bstream, endianness)
    data = ('' if context == '' else '.') + context + get_data(bstream, endianness)
    return data

def get_ordered_dict(bstream, endianness, val_type):
    keys = get_data(bstream, endianness)
    vals = get_data(bstream, endianness)
    if isinstance(keys, pandas.DataFrame):
        data = pandas.concat([keys, vals], axis = 1)
        data = data.set_index(list(keys.columns))
    else:    
        data = OrderedDict(zip(keys, vals))
    return data    

def get_data(bstream, endianness):
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
    bstream = ConstBitStream(bits)
    endianness, size = get_header(bstream)
    while (bstream.pos < 8*size):
        data = get_data(bstream, endianness)
    return data    

int_types = {-11:get_symbol,
    -1:get_bool,
    -13:get_month,
    -14:get_date,
    -15:get_datetime,
    -16:get_nanodatetime,
    -20:[],
    1:get_bool_list,
    10:get_char_list,
    11:get_symbol_list,
    13:get_month_list,
    14:get_date_list,
    15:get_datetime_list,
    16:get_nanodatetime_list,
    20:[],
    98:get_table,
    99:get_dict,
    100:get_lambda_func,
    127:get_ordered_dict
    }


