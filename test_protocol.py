'''

tests come from examples given here:
    http://code.kx.com/wiki/Reference/ipcprotocol
types are found here:
    http://www.kx.com/q/d/q1.htm
'''
import pandas
import datetime
import numpy as np

from collections import OrderedDict
from nose.tools import assert_almost_equal, assert_items_equal
from protocol import parse, format_bits

def assert_all_equal(input_a, input_b):
    if len(input_a) != len(input_b):
        return False
    for i in range(len(input_a)):
        if not (input_a[i] == input_b[i]).all():
            return False
    return True    
def test_int():
    data = 1
    bits = b'0x010000000d000000fa01000000'
    assert data == parse(format_bits(data))
    assert data == parse(bits)
    assert bits == format_bits(data).__str__()

def test_int_vector():
    data = np.array([1])
    bits = b'0x010000001200000006000100000001000000'
    assert data == parse(format_bits(data))
    assert data == parse(bits)
    assert bits == format_bits(data).__str__()

def test_byte_vector():
    data = np.array([0,1,2,3,4], dtype=np.int8)
    bits = b'0x01000000130000000400050000000001020304'
    assert (data == parse(format_bits(data))).all()
    assert (data == parse(bits)).all()
    assert bits == format_bits(data)
     
def test_list():
    data = [np.array([0,1,2,3,4], dtype=np.int8)]
    bits = b'0x01000000190000000000010000000400050000000001020304'
    assert_all_equal(data, parse(format_bits(data)))
    assert_all_equal(data, parse(bits))
    assert bits == format_bits(data)
    
def test_simple_dict():
    data = {'a':2,'b':3}
    bits = b'0x0100000021000000630b0002000000610062000600020000000200000003000000'
    assert data == parse(format_bits(data)) 
    assert data == parse(bits) 
    assert bits == format_bits(data).__str__()
    
def test_ordered_dict():
    data = OrderedDict({'a':2,'b':3})
    bits = b'0x01000000210000007f0b0102000000610062000600020000000200000003000000'
    assert data == parse(format_bits(data)) 
    assert data == parse(bits) 
    assert bits == format_bits(data).__str__()

def test_dict_vector():
    data = {'a':[2], 'b':[3]}
    bits = b'0x010000002d000000630b0002000000610062000000020000000600010000000200000006000100000003000000'
    assert data == parse(format_bits(data)) 
    assert data == parse(bits) 
    assert bits == format_bits(data).__str__()

def test_table_simple():
    data = pandas.DataFrame([{'a':2,'b':3}])
    bits = b'0x010000002f0000006200630b0002000000610062000000020000000600010000000200000006000100000003000000'
    assert data == parse(format_bits(data)) 
    assert bits == format_bits(data).__str__()
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

def test_real():
    data = 2.3
    bits = b'0x010000000d000000f833331340'
    assert_almost_equal(data, parse(bits))

def test_real_vector():
    data = [2.3]
    bits = b'0x010000001200000008000100000033331340'
    assert_almost_equal(data[0], parse(bits)[0])
    
def test_float():
    data = 2.3
    bits = b'0x0100000011000000f76666666666660240'
    assert_almost_equal(data, parse(bits))

def test_float_vector():
    data = [2.3]
    bits = b'0x01000000160000000900010000006666666666660240'
    assert_almost_equal(data[0], parse(bits)[0])

def test_month():
    data = datetime.datetime(2012,6,1)
    bits = b'0x010000000d000000f395000000'
    assert data == parse(bits)
    
def test_month_vector():
    data = [datetime.datetime(2012,6,1)]
    bits = b'0x01000000120000000d000100000095000000'
    assert data == parse(bits)
    
def test_date():
    data = datetime.datetime(2012,6,8)
    bits = b'0x010000000d000000f2be110000'
    assert data == parse(bits)
    
def test_date_vector():
    data = [datetime.datetime(2012,6,8)]
    bits = b'0x01000000120000000e0001000000be110000'
    assert data == parse(bits)
    
def test_minute():
    data = datetime.time(8,31)
    bits = b'0x010000000d000000efff010000'
    assert data == parse(bits)
    
def test_minute_vector():
    data = [datetime.time(8,31)]
    bits = b'0x0100000012000000110001000000ff010000'
    assert data == parse(bits)
    
def test_second():
    data = datetime.time(8,31,53)
    bits = b'0x010000000d000000eef9770000'
    assert data == parse(bits)
    
def test_second_vector():
    data = [datetime.time(8,31,53)]
    bits = b'0x0100000012000000120001000000f9770000'
    assert data == parse(bits)
    
def test_time():
    data = datetime.time(8,31,53,981)
    bits = b'0x010000000d000000ed7da8d401'
    assert data == parse(bits)
    
def test_time_vector():
    data = [datetime.time(8,31,53,981)]
    bits = b'0x01000000120000001300010000007da8d401'
    assert data == parse(bits)
    
def test_datetime():
    data = datetime.datetime(2012,6,8,8,31,53,981000)
    bits = b'0x0100000011000000f1e9941f015bbeb140'
    assert data == parse(bits)
    
def test_datetime_vector():
    data = [datetime.datetime(2012,6,8,8,31,53,981000)]
    bits = b'0x01000000160000000f0001000000e9941f015bbeb140'
    assert data == parse(bits)
    

