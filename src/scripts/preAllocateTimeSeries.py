"""

At the end of each day, get a list of {keyword, mgrs} that we should build time series docs for to relieve the overhead of doing these during processing time.
Build the ability to get that list from several sources
- Function that gets a list of {keyword, mgrs} : count and take the top 10 %
- Function that gets that list for the previous day
- Function that gets that list based on a filter of the same day of the week previously.

Set it up with a config file.
Set it up as a daemon for the low activity times - very beginning of the day.

Is there a way of validating the predictions...?

Arguments:

1. The current UTC datetime
2. Baseline types - which determine what functions to call.
3. 

ACTUAL FUNCTIONS

1. Main - retrieves the config parameters
        - gets the current day from utcnow() and replace hour,min,second,microsecond.
        - takes in the type of predictors to use
        - calls those predictors to retrieve different lists of {keyword,mgrs}
        
2. Cleanup - removing those documents that didn't get populated. Search for where data array is all 0. Remove() them from timeseries. Count/write to file the
   number that we cleaned up.

3. getActive(lookback) - gets all combinations of mgrs and keyword ever. Lookback allows us to specify a certain period during which we saw the combinations.

4. getActiveLastWeek() - get the start and stop that correspond to this time last week. Submit those as queries...

5. processPredictions({predictions=[keyword:mgrs}])

    - takes in a list of predictions
    - calls 

"""
#! /usr/bin/python

import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx)

import os
import ConfigParser
import datetime

import mdb                  # Custom library for mongo interaction
import geojson              # Non-standard FOSS library
import numpy as np
import json
from bson.code import Code
    
#================================================================================================

class params():
    ''' Reads in parameters from a config file and makes them
        available through object attributes.. '''
    
    def __init__(self, path, file):
    
        self.errors = []
         
        config = ConfigParser.ConfigParser()
        try:
            config.read(os.path.join(path, file))
        except Exception, e:
            self.errors.append(e)
            
        # Misc parameters
        try:
            self.host   = config.get("mongo", "host")
            self.port   = int(config.get("mongo", "port"))
            self.db     = config.get("mongo", "db")
            self.coll   = config.get("mongo", "collection")
        except Exception, e:
            self.errors.append(e)
    
        # Other parameters
        try:
            baseline                 = json.loads(config.get("default", "baseline"))
            self.baselineTypes       = baseline['methods']
            self.verbose             = config.get("default", "verbose")
            self.mgrsPrecision       = config.getint("default", "mgrsPrecision")
            self.nothingFoundKeyword = config.get("default", "nothingFoundKeyword")
            
        except Exception, e:
            self.errors.append(e)

#----------------------------------------------------------------------------------------------------      

def cleanUpBadPredictions(collectionHandle):
    ''' Clean up pre-allocated documents that weren't used 
    
    TO MAKE THIS VERY EFFECTIVE, THERE SHOULD BE A **SPARSE** INDEX ON TIMESERIES ON A FIELD THAT RECORDS
    WHETHER THE DOCUMENT HASN'T BEEN UPDATED. FIELD IS CALLED 'blank'. If data gets entered into the timeseries, 
    we need to remove this field altogether. As such, the sparse index will not look include the documents that DO have
    data. So, when this function removes documents that have a 'blank' field, it'll use the sparse index. db.timeseries.remove({'blank':true}) 
    
    Steps needed:
    1. When inserting a new timeseries through preAllocation (this script), include a field called 'blank' and set it to true.
    2. Create a sparse index on the timeseries collection against the 'blank' key.
    3. Make the timeseries inserter/updater function remove the 'blank' field from timeseries document. It will only already exist in preAllocated timeseries
    4. Make this function remove any timeseries document where blank=true.
    5. Possibly count the number of documents it removed. And possibly count the total number of documents for the day - FOR ACCURACY ASSESSMENT STATS.
    . '''
    
    # Only get those docs where the blank field is still active
    filter = {'blank':True}
    
    # Remove those docs where blank field exists
    collectionHandle.remove(filter)
    

#----------------------------------------------------------------------------------------------------      

def buildBlankData(hours=24, mins=60):
    ''' Builds an attribute representing a day (default). Might want to have the
        array start life as a flat (1D) array and then resize it. This would be 
        consistent with the output format - a flat 1D array. ***NOT TESTED*** '''
    
    # A blank numpy array that is converted into a python list
    arr = np.zeros((hours,mins), np.int)
    d = arr.tolist()
    return d

#----------------------------------------------------------------------------------------------------      

def insertDoc(collectionHandle, mgrs, mgrsPrecision, keyword, source, start, blankData):
    ''' Builds a mongo document and inserts it. TESTED '''
    
    doc = {'mgrs'          : mgrs,
           'mgrsPrecision' : mgrsPrecision,
           'keyword'       : keyword,
           'start'         : start,
           'data'          : blankData,
           'blank'         : True
           }

    try:
        response = collectionHandle.insert(doc)
    except:
        print "Failed to insert object into mongo 'timeseries' collection."
        response = None

    return response

#----------------------------------------------------------------------------------------------------      

def getCommonMgrsAndKeyword(collHandle, mgrsPrecision, nothingFound, today, lookback):
    ''' Retrieves a list of commonly used mgrs/keyword timeseries docs yesterday '''

    # Get yesterday's date
    queryStart = today - datetime.timedelta(days=lookback)
    condition = {'mgrsPrecision':mgrsPrecision, 'start':{'$gte':queryStart}}
    
    key = ['keyword','mgrs']
    initial={'count':0}
    reduce = "function(obj,prev){prev.count++;}"

    results = collHandle.group(key       = key,
                               initial   = initial, 
                               reduce    = reduce,
                               condition = condition)
    
    # List to store output results
    outResults = []
    maxCount = 1
    minCount = 2
    
    # Return the results
    for res in results:
        
        # If do not want 'nothingFound' keywords built, then drop them from this list
        if nothingFound:
            if str(res['keyword']) != str(nothingFound):
                outResults.append(res)
        else:
            outResults.append(res)
            
        # Record the min and max count to allow some thresholding later on
        if res['count'] > maxCount:
            maxCount = res['count']
        if res['count'] < minCount:
            minCount = res['count']
            
    return outResults, minCount, maxCount

#----------------------------------------------------------------------------------------------------      

def main():
    
    # Config file parameters
    pathIn = '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/config/'
    fileIn = 'preAllocateTimeSeries.cfg'
    
    # Get parameters from config
    p = params(pathIn, fileIn)
    
    # Connect and get db and collection handle
    c, dbh = mdb.getHandle(p.host, p.port, p.db)
    collectionHandle = dbh[p.coll]

    # Current datetime
    #today = datetime.datetime.utcnow().replace(hour=0, minute=0,second=0,microsecond=0)
    today = datetime.datetime(2011,5,1)
    
    # Build some blank data
    blankDataArr = buildBlankData()
    
    # A list to hold the timeseries we need to pre-allocate for
    
    # Get pairs to be pre-allocated from yesterday - lookback is in days
    if 'yesterday' in p.baselineTypes:
        preAllocate, minCount, maxCount = getCommonMgrsAndKeyword(collectionHandle, p.mgrsPrecision, p.nothingFoundKeyword, today, lookback=1)
    
    # Now loop the keyword/mgrs pairs and build new timeseries documents for today
    for item in preAllocate:
        response = insertDoc(collectionHandle, item['mgrs'], p.mgrsPrecision, item['keyword'], 'twitter', today, buildBlankData())
        
    mdb.close(c, dbh)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    
    main()