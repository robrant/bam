

import datetime

def truncateTimeStamp(timeStamp, unit, precision):
    ''' Truncates the timestamp to a certain precision'''
    
    if unit == 'hour':
        val = timeStamp.hour
    elif unit == 'minute':
        val = timeStamp.minute
    elif unit == 'second':
        val = timeStamp.second
    
    while val % int(precision) != 0:
        val -= 1
    
    if unit == 'hour':
        timeStamp = timeStamp.replace(hour=val, minute=0, second=0, microsecond=0)
    elif unit == 'minute':
        timeStamp = timeStamp.replace(minute=val, second=0, microsecond=0)
    elif unit == 'second':
        timeStamp = timeStamp.replace(second=val, microsecond=0)

    return timeStamp

x = datetime.datetime(2011, 1, 1, 12, 3, 3)
trunc_timeStamp = truncateTimeStamp(x, unit='minute', precision=10)
print trunc_timeStamp

x = datetime.datetime(2011, 1, 1, 11, 13, 3)
trunc_timeStamp = truncateTimeStamp(x, unit='hour', precision=12)
print trunc_timeStamp

x = datetime.datetime(2011, 1, 1, 11, 13, 47)
trunc_timeStamp = truncateTimeStamp(x, unit='second', precision=5)
print trunc_timeStamp


"""

This gets a count per keyword
db.timeseries.group({key:{keyword:true}, initial:{count:0}, reduce:function(obj,prev){prev.count++;}})
                    
This gets a count per keyword/mgrs combination
db.timeseries.group({key:{keyword:true, mgrs:true}, initial:{count:0}, reduce:function(obj,prev){prev.count++;}})

This gets a count per keyword/mgrs combination - where the mgrs precision = 8
db.timeseries.group({key:{keyword:true, mgrs:true}, initial:{count:0}, reduce:function(obj,prev){prev.count++;}, cond:{mgrs_precision:8}})

This gets a count per mgrs/keyword with a condition on MGRS Precision and for a specific time range
NOTE - AT THIS POINT ITS JUST RETURNING DOCUMENTS - SO WILL ONLY EVER BE PRECISE ENOUGH FOR A DAY

db.timeseries.group({key:{keyword:true, mgrs:true},
                     initial:{count:0},
                     reduce:function(obj,prev){prev.count++;},
                     cond:{mgrs_precision:10,
                           start:{'$gt':ISODate("2011-05-01T00:00:00")},
                           start:{'$lt':ISODate("2011-05-01T00:01:00")},
                           }
                     })

db.timeseries.group({key:{keyword:true, mgrs:true},
                     initial:{count:0},
                     reduce:function(obj,prev){prev.count++;},
                     cond:{keyword:'sick'}
                     })


db.baseline.group({key:{keyword:true, mgrs:true},
                     initial:{count:0},
                     reduce:function(obj,prev){prev.count++;}
                     })



This example does the same but using the data.0.0 notation, aggregates by the hour/minutes as well
"""