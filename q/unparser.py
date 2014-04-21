from bitstring import pack, ConstBitStream
import pandas
import numpy as np
import datetime
from protocol import types, inv_types, header_format, MILLIS, Y2KDAYS, NULL, BYTE, INT
from utils import str_convert, format, format_list, get_header, get_date_from_q, get_hour, format_raw_list 
from collections import OrderedDict

def format_bits(data, endianness = 'le', with_index=False, sort_on=None, async=False, symbol = True, function = False, short = False):
    endian = 1 if endianness == 'le' else 0
    data_format = header_format.format(endianness)
    data = parse_on_the_wire(data, endianness, with_index=with_index, sort_on=sort_on, symbol=symbol, function = function, short=short)
    length = len(data)/8 + 8
    objects = {'endian':endian, 'async':1 if async else 0, 'length': length, 'data':data}
    bstream = pack(data_format, **objects)
    return bstream

#This is looking like it needs a refactor!
def parse_on_the_wire(data, endianness, attributes = 0, with_index=False, sort_on = None, symbol = True, function = False, short = False):
    if with_index and type(data) == pandas.DataFrame:
        keys = parse_on_the_wire(pandas.DataFrame(data.index), endianness, attributes, False, True if sort_on else None, short=short)
        vals = parse_on_the_wire(data, endianness, attributes, False, short=short)
        data_format = 'int{0}:8=type, bits'.format(endianness)
        bstream = pack(data_format, (keys+vals), type='127' if sort_on else '99')
    elif isinstance(data,np.ndarray):
        dtype = inv_types[data.dtype.type]
        if data.dtype.type == np.object_:
            data_format = 'int{0}:8=type, int{0}:8=attributes, int{0}:32=length, bits'.format(endianness)
            bstream = pack(data_format, sum([parse_on_the_wire(i, endianness, short=short) for i in data]), type = dtype[0], attributes=attributes, length=len(data))
        else:
            data_format = 'int{0}:8=type, int{0}:8=attributes, int{0}:32=length, {3}*{1}{0}:{2}'.format(endianness, dtype[1], dtype[2], len(data))
            bstream = pack(data_format, *data, type=dtype[0], attributes=attributes, length=len(data))
    elif isinstance(data, list):
        type_set = set([type(i) for i in data])
        if len(type_set) == 1 and not list(type_set)[0] == np.ndarray:
            if short:
                temp = list(type_set)[0]
                if temp == float:
                    dtype = inv_types['float']
                if temp == int:    
                    dtype = inv_types['int']
            else:    
                dtype = inv_types[list(type_set)[0]]
            if list(type_set)[0] == str or list(type_set)[0] == list:
                data_format = 'int{0}:8=type, int{0}:8=attributes, int{0}:32=length, bits'.format(endianness)
                bstream = pack(data_format, sum([parse_on_the_wire(i, endianness, short=short) for i in data]), type = -dtype[0], attributes=attributes, length=len(data))
            else:
                data_format = 'int{0}:8=type, int{0}:8=attributes, int{0}:32=length, {3}*{1}{0}:{2}'.format(endianness, dtype[1], dtype[2], len(data))
                bstream = pack(data_format, *data, type=-dtype[0], attributes=attributes, length=len(data))
        else:
            dtype = inv_types[type(data)]
            data_format = 'int{0}:8=type, int{0}:8=attributes, int{0}:32=length, bits=data'.format(endianness)
            bits = sum([parse_on_the_wire(i, endianness, short=short) for i in data])
            bstream = pack(data_format, data=bits, type=-dtype[0], attributes=attributes, length=len(data))
    elif type(data) == dict:
        dtype = inv_types[type(data)]
        data_format = 'int{0}:8=type, bits=data'.format(endianness)
        keys = parse_on_the_wire(data.keys(), endianness, short=short)
        vals = parse_on_the_wire(data.values(), endianness, short=short)
        bits = keys + vals
        bstream = pack(data_format, data=bits, type=dtype[0], attributes=attributes, length=len(data))
    elif type(data) == OrderedDict:
        dtype = inv_types[type(data)]
        data_format = 'int{0}:8=type, bits=data'.format(endianness)
        keys = parse_on_the_wire(data.keys(), endianness, 1, short=short)
        vals = parse_on_the_wire(data.values(), endianness, short=short)
        bits = keys + vals
        bstream = pack(data_format, data=bits, type=dtype[0], attributes=attributes, length=len(data))
    elif isinstance(data, str) and function:
        context, function = data.split('{') 
        context = context.replace('.','')
        function = '{' + function
        data_format = 'int{0}:8=lambdatype, bits=context, bits=function'
        bstream = pack(data_format.format(endianness), lambdatype=100, context = parse_on_the_wire(context, endianness), function = parse_on_the_wire(function, endianness, symbol=False) ) 
    elif isinstance(data, str) and symbol:
        bstream = pack('{0}*hex:8'.format(len(data)),*[hex(ord(i)) for i in data]) + ConstBitStream(b'0x00')
    elif isinstance(data, str):    
        dtype = inv_types['str']
        data_format = 'int{1}:8=type, int{1}:8=attributes, int{1}:32=length, {2}*hex:8'
        bstream = pack(data_format.format('', endianness, len(data)),type=-dtype[0], attributes=attributes, length=len(data), *[hex(ord(i)) for i in data]) 
    elif type(data) == pandas.DataFrame:
        is_sorted = 1 if sort_on else 0
        dtype = inv_types[type(data)]
        data_format = 'int{0}:8=type, int{0}:8=tabattrib, int{0}:8=dicttype, bits=cols,int{0}:8=typearray, int{0}:8=attributes, int{0}:32=length, bits=vals'.format(endianness)
        cols = parse_on_the_wire(data.columns.values, endianness, short=short)
        vals = sum(parse_on_the_wire(col.values, endianness, 3 if i==sort_on else 0, short=short) for i,col in data.iterkv())
        #    indexes = parse_on_the_wire(data.index.values, endianness, 3)
        bstream = pack(data_format, cols=cols, type=dtype[0], typearray=0, attributes=0, length=len(data.columns), vals=vals, tabattrib=is_sorted, dicttype=99)
    elif type(data) == float and short:
        dtype = inv_types['float']
        data_format = 'int{0}:8=type, {1}{0}:{2}'.format(endianness, dtype[1], dtype[2])
        bstream = pack(data_format, data, type=dtype[0])
    elif type(data) == int and short:
        dtype = inv_types['int']
        data_format = 'int{0}:8=type, {1}{0}:{2}'.format(endianness, dtype[1], dtype[2])
        bstream = pack(data_format, data, type=dtype[0])
    else:    
        dtype = inv_types[type(data)]
        data_format = 'int{0}:8=type, {1}{0}:{2}'.format(endianness, dtype[1], dtype[2])
        bstream = pack(data_format, data, type=dtype[0])
    return bstream


