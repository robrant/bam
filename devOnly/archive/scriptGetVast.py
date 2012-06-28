'''
Processing about 1000 into keyword objects in about 0.2 seconds.
Processed 1023057 tweets in 0:11:48.339550 (11 minutes)
'''

import sys
import copy

importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx)

import datetime
import os
import time # Can be used for trickle of feed onto a Q
import subprocess

# VAST tweet objects
from baseObjects import vastTweet

# Need the base objects keyword for converting the tweets to keywords - testing mainly
from baseObjects import keyword

# The class/fns being tested
from scriptTweets2Keywords import processTweet

# JMS code
import jmsCode

# Handles the building of time series objects in mongo
from timeSeriesDoc import *

# Mongo connection wrapper
import mdb

# Baseline Processing
import baselineProcessing as bl

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
        dtg = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
    #finally:
    #    print "="*30
    #    print "ID number %s doesn't have valid timestamp: %s" %(tId, dt)
    #    print "="*30
    #    return None
    
    return dtg


#------------------------------------------------------------------------------------------ 

def callBaseliner(scriptFile, host, port, db, kwObj, baselineParameters, mac=None):
    ''' Kicks of a command line operation and doesn't wait for the response'''
    
    # Mac has a weird 32-bit vs 64-bit thing that needs this line to run in 32-bit mode on a 32 bit machine
    if mac:
        pythonVersion = 'arch -i386 /usr/bin/python2.6'
    else:
        pythonVersion = 'python'
        
    # Kick off the baseline processing via subprocess
    cmds = [pythonVersion, scriptFile,
            '-H', host,
            '-p', str(port),
            '-d', db,
            '-m', kwObj.mgrs,
            '-M', str(kwObj.mgrsPrecision),
            '-t', kwObj.timeStamp.strftime("%Y-%m-%dT%H:%M:%S"),
            '-k', str(kwObj.keyword),
            '-u', baselineParameters[0],
            '-v', str(baselineParameters[1])]
    
    command = ' '.join(cmds)
    
    call = subprocess.Popen(command, shell=True)

    return call

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
    db = 'bam'
    host = 'localhost'
    port = 27017
    
    start = datetime.datetime.utcnow()
    tweetProcessTimes = datetime.timedelta(seconds=0)
    
    blUnits     = 'minute'
    blPrecision = 10
    baselineParameters = [blUnits, blPrecision] 
    mgrsPrecision = 2
    
    #dripRate = 1.5
    
    # JMS destination
    #destination = '/topic/test.vasttweets'
    #hostIn      = 'localhost'
    #portIn      = 61613

    # Reset the collections
    c, dbh = mdb.getHandle()
    dbh = mdb.setupCollections(dbh, dropCollections=True)         # Set up collections
    dbh = mdb.setupIndexes(dbh)
    
    #jms = jmsCode.jmsHandler(hostIn, portIn, verbose=True)
    # Make the JMS connection via STOMP and the jmsCode class
    #jms.connect()
     
    path = "/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/data/"
    #fName= "MicroblogsSample.csv"
    fName= "MicroblogsOrdered.csv"
    tweetStats = 'tweetStatsFile_50000.csv'
    tptFile = open(path+tweetStats, 'w')
    
    # The script used to generate the baseline
    baselinePath = '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/'
    baselineScript = 'subprocessBaseline.py'
    scriptFile = os.path.join(baselinePath, baselineScript)

    
    f = retrieveFile(path, fName)
    x = 0
    
    # Start time
    earliestTweet = datetime.datetime(2011, 4, 30, 0, 0)
    earliestTweet = time.mktime(time.struct_time(earliestTweet.timetuple()))
    lastTweetTime = earliestTweet
    print "First Tweet Time: ", lastTweetTime
    
    # This speeds things up from seconds to minutes
    speedUpRate = 1000
    
    # Build a blank timeseries array to save it being built everytime
    blankData = buildBlankData(hours=24)
    
    # Loop the lines build tweet objects
    for line in f.readlines():
        
        #print line
        # Extract content from each line
        line = line.rstrip('\r').rstrip('\n').rstrip('\r')

        if x == 0:
            x+=1
            continue
        
        if x % 100 == 0:
            print "processed: ", x
        
        if x >100000:
            print line
            break
            sys.exit(0)
            
        line = line.split(',')
        
        tweetProcessStart = datetime.datetime.utcnow()
        
        tweetId, dt, latLon, text = line
        
        # Get the geos
        geos = getGeos(tweetId, latLon)
        if not geos:
            print "skipping this record - bad or no geos"
            continue
        
        # Get the datetime group into seconds since UNIX time
        dtg = getTime(tweetId, dt)

        if not dtg:
            print "skipping this record - bad or no time"
            continue
        
        # Get the tweettime into seconds from UNIX
        tweetTime = time.mktime(time.struct_time(dtg.timetuple()))
        #print "The time of this tweet", tweetTime
        
        # Get the tweet time in seconds since the last tweet
        sinceLastTweet = tweetTime - lastTweetTime
        #print "Time since last tweet", sinceLastTweet
        
        #delay = sinceLastTweet / speedUpRate
        #print "Delay: ", delay
                
        # Apply a scaling to it
        #time.sleep(delay)
        
        # Assign this tweet time to be the last tweet time
        lastTweetTime = tweetTime
        
        # Build a tweet object
        twt = vastTweet()
        twt.importData(timeStamp=dtg, lat=geos[0], lon=geos[1], text=text, tweetId=tweetId)
        
        #----------------------------------------------------------------------------------
        # PROCESS INTO KEYWORDS
                
        # Build into keywords - skipping a step for development
        kywd = processTweet(twt, mgrsPrecision)
        
        # Add keywords to the list based on hashtags
        kywd.fromHashTag()
        
        # Add keywords to the list based on name lookup
        kywd.fromLookup()

        if len(kywd.keywords) == 0:
            pass
            #print "No matches: ", twt.text
        
        xx = 0
        #Now loop the resultant keywords
        for kwObj in kywd.keywords:
            
            xx += 1
            
            #print "------------------"
            #print kwObj.keyword
            #print kwObj.text
        
            #-------------------------------------------------------
            # Pass keyword object into a class
            #ts = timeSeries(host='localhost', port=27017, db='bam')
            ts = timeSeries(c=c, dbh=dbh)
            ts.importData(kwObj, blockPrecision=24)
    
            success = ts.insertDoc(blankData=blankData, incrementBy=100)
  
            callBaseliner(scriptFile, host, port, db, kwObj, baselineParameters, mac=1)
  
        # METRICS - currently about 0.05 seconds per tweet
        tweetProcessStop = datetime.datetime.utcnow()
        tweetProcessTimes += (tweetProcessStop - tweetProcessStart)
        processDif = (tweetProcessStop - tweetProcessStart) 
        tptFile.write(str(x)+","+str(xx)+","+str(processDif.seconds + processDif.microseconds/1000000.)+"\n")
        #----------------------------------------------------------------------------------
        # SEND TO JMS WITH THIS CODE

        # Convert it into a JSON object
        #jTwt = twt.vastTweet2Json()
        #print jTwt

        # Push the JSON version of the tweet to the JMS
        #jms.sendData(destination, jTwt, x)

        #----------------------------------------------------------------------------------
        
        x += 1
    
        #time.sleep(dripRate)
        
    # Disconnect from the JMS
    #jms.disConnect()    

    end = datetime.datetime.utcnow()
    dif = end - start
    
    print "Total Tweet Process Time: %s" %tweetProcessTimes.seconds
    print "Average Tweet process time: %s" % (float(tweetProcessTimes.seconds)/float(x))

    print "Tweet Processed: %s" %x
    print "Total Process Time: %s" %(dif)
    
    # Close the mongo connection
    mdb.close(c, dbh)
    f.close()
    tptFile.close()
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    
    import profile
    profile.run('main()')


# DONE
# -----
# SUCCESSFULLY PUT THE TWEET OBJECTS ON THE JMS
# SUCCESSFULLY TAKE A TWEET OBJECT INTO KEYWORD OBJECTS

# TO DO
# -----

# GEOGRAPHIC LOOKUP FOR LANDMARKS WHERE GEOS AREN'T PRESENT
# SUBSCRIBE TO THE TOPIC TO GET TWEETS BACK AND THEN PROCESS THEM INTO KEYWORD OBJECTS
# USE A QUEUE IN JMS TO STORE KEYWORDS TO BE PROCESSED
# USE A PYTHON QUEUE?

# DO ALL THE STUFFING INTO MONGO








