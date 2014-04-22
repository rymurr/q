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
    need some docstrings
    add in async/concurrency stuff for speed
    profile!
    integrate back into connection class and do full tests    
    clarify handling of OrderedDict
    add in pd.Series
    clarify handling of sorted and keyed tables
    add indicies (associated with keys)
'''
import pandas
import datetime
import numpy as np
from bitstring import ConstBitStream
from collections import OrderedDict
from time import mktime

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
        -16:('int', '64'), #nano datetime
        16:('int', '64'), #nano datetime vector
        -17:('int', '32'), #hour
        17:('int', '32'), #hour vector
        -18:('int', '32'), #second
        18:('int', '32'), #second vector
        -19:('int', '32'), #time
        19:('int', '32'), #time vector
         0:('list','0'), #list
        }

inv_types = {
        bool: (1, 'int', '8'),
        np.bool: (-1, 'int', '8'),
        np.int8: (4, 'int','8'), 
        'int': (-5, 'int', '16'),
        np.int16: (5, 'int', '16'), 
        long: (-7, 'int', '64'),
        np.int64: (7, 'int', '64'),
        float: (-9, 'float', '64'),
        'float': (-8, 'float', '32'),
        np.float32: (8, 'float', '32'),
        np.float64: (8, 'float', '64'),
        int: (-6, 'int', '32'),
        list: (0, '', ''),
        dict: (99, '', ''),
        str: (-11, 'hex', '8'),
        'str': (-10, 'hex', '8'),
        OrderedDict: (127, '', ''),
        np.int64: (6, 'int', '32'),
        np.int8: (4, 'int', '8'),
        np.object_: (11, 'hex', 8,),
        pandas.DataFrame: (98, '', ''),
        }
INT = -6
BYTE = -4
NULL = ConstBitStream('0x00')
Y2KDAYS = datetime.datetime(2000,1,1).toordinal()
Y2KMILLIS = mktime(datetime.datetime(2000,1,1).utctimetuple())
MILLIS = 8.64E7 

#header format
header_format = 'int{0}:8=endian, int{0}:8=async, pad:16, int{0}:32=length, bits=data'



