'''
Processing about 1000 into keyword objects in about 0.2 seconds.
Processed 1023057 tweets in 0:11:48.339550 (11 minutes)
'''

import sys

importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx)

import datetime
import os
import time # Can be used for trickle of feed onto a Q

# VAST tweet objects
from baseObjects import vastTweet

# Need the base objects keyword for converting the tweets to keywords - testing mainly
from baseObjects import keyword

# The class/fns being tested
from scriptTweets2Keywords import processTweet

# JMS code
import jmsCode

# Handles the building of time series objects in mongo
from timeSeriesDoc import timeSeries

# Mongo connection wrapper
import mdb

# Baseline Processing
import baselineProcessing as bl

import operator


#------------------------------------------------------------------------------------------ 
#------------------------------------------------------------------------------------------ 


def retrieveFile(path, fName):
    ''' Retrieves the VAST dataset file into a file object Tests Written. '''
    
    try:
        f = open(os.path.join(path, fName), 'r')
    except:
        print "Failed to open the file."
        
    return f

#------------------------------------------------------------------------------------------ 

def getGeos(tId, location):
    ''' Check the geos are valid and split them up.  Tests Written.  '''
    
    geos = location.split(' ')
    if len(geos) > 1:
        try:
            lat, lon = float(geos[0]), float(geos[1]) 
        except:
            print "="*30
            print "ID number %s doesn't have valid geos: %s" %(tId, location)
            print "="*30
            return None
            
    else:
        print "="*30
        print "ID number %s doesn't have valid geos: %s" %(tId, location)
        print "="*30
        return None
    
    return [lat, lon]


#------------------------------------------------------------------------------------------ 

def getTime(tId, dt):
    ''' Check the timestring is valid. Tests Written. '''

    # Get the time info (#5/20/2011 16:55)
    try:
        dtg = datetime.datetime.strptime(dt, '%m/%d/%Y %H:%M')
    except:
        print "="*30
        print "ID number %s doesn't have valid timestamp: %s" %(tId, dt)
        print "="*30
        return None
    
    return dtg

#------------------------------------------------------------------------------------------ 

def sortTable(table, col=1):
    
    return sorted(table, key=operator.itemgetter(col))

#------------------------------------------------------------------------------------------ 

def main(): 
    '''
    Script to build tweet objects from the VAST dataset and place them on a Queue and/or JMS
    for testing purposes.
    
    LIKELY SPEED IMPROVEMENTS:
    - BUILDING BLANK ARRAYS IN THE TIME SERIES TAKES A WHILE
    - PUTTING THE KEYWORDS IN A QUEUE, HAVING SET UP THE THREADS TO PROCESS EACH ONE.
    - ANY DUPLICATION CHECKS?
    
    
    
    '''
    
    start = datetime.datetime.utcnow()
    tweetProcessTimes = datetime.timedelta(seconds=0)
    
    #dripRate = 1.5
    
    # JMS destination
    destination = '/topic/test.vasttweets'
    hostIn      = 'localhost'
    portIn      = 61613

    # Reset the collections
    c, dbh = mdb.getHandle()
    dbh = mdb.setupCollections(dbh, dropCollections=True)         # Set up collections

    #jms = jmsCode.jmsHandler(hostIn, portIn, verbose=True)
    # Make the JMS connection via STOMP and the jmsCode class
    #jms.connect()
     
    path = "/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/data/"
    #fName= "MicroblogsSample.csv"
    fName= "Microblogs.csv"
    outFName = "MicroblogsOrdered.csv"
    
    f = retrieveFile(path, fName)
    fo = open(os.path.join(path, outFName), 'w')
    
    x = 0
    
    # Start time
    earliestTweet = datetime.datetime(2011, 5, 18, 13, 25)
    earliestTweet = time.mktime(time.struct_time(earliestTweet.timetuple()))
    lastTweetTime = earliestTweet
    print "First Tweet Time: ", lastTweetTime
    
    # This speeds things up from seconds to minutes
    speedUpRate = 60.0
    records = []
    
    # Loop the lines build tweet objects
    for line in f.readlines():
        
        #print line
        # Extract content from each line
        line = line.rstrip('\r').rstrip('\n').rstrip('\r')
        
        if x == 0:
            x+=1
            continue
        
        if x % 1000 == 0:
            print "processed: ", x
        #if x > 1000:
        #    break
        #    sys.exit(0)
            
        line = line.split(',')
        
        tweetId, dt, latLon, text = line
        
        # Get the datetime group into seconds since UNIX time
        dtg = getTime(tweetId, dt)

        if not dtg:
            continue

        record = [tweetId, dtg, latLon, text]
        records.append(record)
        
        x += 1
    
    f.close()
    
    sortedTable = sortTable(records, col=1)

    
    # Now loop the sorted list and write out to a new file
    for record in sortedTable:
        

        lineOut = "%s,%s,%s,%s\n" %(record[0], record[1], record[2], record[3])
        
        fo.write(lineOut)
    
    f.close()
            
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    main()

