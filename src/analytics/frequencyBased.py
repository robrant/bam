import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx) 

import numpy as np
import numpy.ftt as fft
from matplotlib import pyplot as pp
import mdb
import datetime
from baselineProcessing import buildFullArray
from timeSeriesDoc import buildBlankData

def fftDev(arr):
    ''' Builds an fft from an input array '''
    
    # Do the fft
    fftArr = fft.fft(arr)
    n = len(fftArr)
    # Plot the fft results
    pp.plot(fftArr)
    pp.show()
    
    # Get the absolute amplitude/power from the fft for half of the fft results
    power = abs(fftArr[1:(n/2)])**2
    nyquist = 1.0/2
    freq = np.array(range(n/2))/(n/2.0)*nyquist
    period = 1.0/freq
    
    # Reformats the x axis into time, rather than frequency
    pp.plot(period[1:len(period)], power)
    
    axlims = pp.axis()
    v = [0, 50, axlims[2], axlims[3]]
    pp.axis(v)
    
    pp.grid()
    pp.show()
    

#----------------------------------------------------------------------------

#========================

host = 'localhost'
port = 27017
db = 'bam'
collection = 'timeseries'
lookback = datetime.timedelta(days=5)
queryEnd = datetime.datetime(2011,05,04)
queryStart = queryEnd - lookback
mgrs = None
mgrsPrecision = None
blankDay = buildBlankData()

#========================

print queryStart, queryEnd

# Get a mongo db handle
c, dbh = mdb.getHandle(host=host, port=port, db=db)

# Get a collection handle
collHandle = dbh[collection]

# Query based on a keyword only
keyword = 'sick'

query = {'keyword':keyword,
         'start':{'$gte' : queryStart},
         'start':{'$lte'  : queryEnd}}

if mgrs:
    query['mgrs'] = mgrs
if mgrsPrecision:
    query['mgrsPrecision'] = mgrsPrecision
    
res = collHandle.find(query).sort({'start':1}) 
resultSet = [r for r in res]

arr = buildFullArray(resultSet, queryEnd, lookback, blankDay, flat=1)

fftDev(arr)

"""
TESTS/EXPERIMENTS

1. Check the array coming back is sensible
2. See what an FFT looks like for a keyword
3. See what an FFT looks like for all documents - need to write some aggregation bits in here.
4. What does the periodogram look like?
5. Build a JTF plot of the frequency data


"""