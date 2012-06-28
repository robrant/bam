import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx)

# STANDARD
import datetime
import os
import time # Can be used for trickle of feed onto a Q
import ConfigParser

# NON STANDARD FOSS
import json
import stomp

# CUSTOM LIBS
from baseObjects import eventRecord             # The event object (tweet, photo metadata, etc)
from baseObjects import processEvent            # Building the keywords from a single event      
from timeSeriesDoc import *                     # Build a timeseries object for recording activity
import mdb                                      # Mongo connection wrapper
from baselineProcessing import baseline         # The subprocess caller

"""
Description:
============
An extended stomp listener that takes messages from a JMS queue, builds 
tweet objects, splits the tweet object into constituent keyword objects
and records activity per keyword/mgrs cell/day in a timeseries object.
The timeseries object (which stores counts/day/mgrs/keyword) then inserts
them into a mongo collection.


Improvements:
=============

"""

#------------------------------------------------------------------------------------------ 

class keywordListener(stomp.ConnectionListener):

    def __init__(self):
        ''' Sets up the config information, database connection and builds
            a blank data array for easy inserting. '''

        # Reads in a load of config information
        path = "/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/docs/"
        file = "keywordListenerConfig.cfg"
        self.getConfig(path, file)
        
        # Handles the mongo connections
        self.c, self.dbh = mdb.getHandle(db=self.db, host=self.host, port=self.port)
        
        # Build a blank timeseries array to save it being built everytime
        self.blankData = buildBlankData(hours=24)
        
#------------------------------------------------------------------------------------------ 

    def getConfig(self, path, file):
        ''' Reads in a list of keywords from a config file. '''
        
        config = ConfigParser.ConfigParser()
        try:
            config.read(os.path.join(path, file))
            out = 1
        except:
            print "Failed to read the config file for keyword lookup."
            out = None
            
        # Misc parameters
        self.mgrsPrecision  = int(config.get("misc", "mgrsPrecision"))
        self.increment      = int(config.get("misc", "increment"))
        terms               = config.get("misc","terms")
        self.lookup         = json.loads(terms)
        self.source         = config.get("misc", "source")
        self.nothingFound   = config.get("misc", "nothingFound")
        self.cleanUpWorker  = config.getboolean("misc", "cleanup")
        
        # Parameters used for mongo connection
        self.db             = config.get("mongo", "db")
        self.host           = config.get("mongo", "host")
        self.port           = int(config.get("mongo", "port"))
        
        # Parameters used in the building of the baseline
        blUnits             = config.get("baseline", "blUnits")
        blPrecision         = int(config.get("baseline", "blPrecision"))
        self.baselineParameters = [blUnits, blPrecision] 

        # The script used to generate the baseline
        baselinePath = config.get("baseline","baselinePath")
        baselineScript = config.get("baseline","baselineScript")
        self.scriptFile = os.path.join(baselinePath, baselineScript)

        return out
    
#----------------------------------------------------------------------
    def on_error(self, headers, message):
        '''If we receive and error from the server'''
        
        print "Received ERROR from the server: %s" %(message)
        
#----------------------------------------------------------------------

    def on_message(self, headers, message):
        '''If we receive a message from the server'''

        # Load the content of the message as JSON
        try:
            eventMessage = json.loads(message)
        except:
            print "Failed to load json message. Message as follows: \n"
            print message
            
        self.processKeyword(eventMessage)
                    
#----------------------------------------------------------------------
        
    def processKeyword(self, eventIn):
        ''' Process tweets coming off JMS into keywords'''
        
        # For processing tweets
        if self.source == 'twitter':
            record = eventRecord('twitter', eventIn)
        
        elif self.source == 'flickr':
            record = eventRecord('flickr', eventIn)
        
        elif self.source == 'instagram':
            record = eventRecord('instagram', eventIn)
        
        elif self.source == 'panoramia':
            record = eventRecord('panoramia', eventIn)
        
        elif self.source == 'foursquares':
            record = eventRecord('foursquares', eventIn)
        else:
            print 'No recognised source of data.'
            sys.exit()
        
        # Goes from single tweet --> n-keywords
        kywd = processEvent(record, self.source, self.mgrsPrecision)
        
        # Add keywords to the list based on hashtags
        kywd.fromHashTag()                  # Get keyword from the hashtags
        kywd.fromLookup(self.lookup)        # Get keywords from a lookup
        
        #=====================================================================================================
        # PUT ADDITIONAL .fromXXX functions in here to get content from other types (tags, keywords, nlp, etc) 
        #=====================================================================================================
        
        # If we don't find anything, build a blank keyword so that the observation isn't lost
        if len(kywd.keywords) == 0:
            kywd.whenNothingFound(tokenName=self.nothingFound)
        
        #Now loop the resultant keywords
        for extractedKeyword in kywd.keywords:
            
            # Pass keyword object into a class
            ts = timeSeries(c=self.c, dbh=self.dbh)
            ts.importData(extractedKeyword, blockPrecision=24)
    
            # Insert the time series document
            success = ts.insertDoc(self.cleanUpWorker, blankData=self.blankData, incrementBy=self.increment)
            
            # Build the baseline objects as we go so that they can be updated at the end of the period.
            base = baseline(mgrs              = extractedKeyword.mgrs,
                            mgrsPrecision     = extractedKeyword.mgrsPrecision, 
                            keyword           = extractedKeyword.keyword, 
                            timeStamp         = extractedKeyword.timeStamp,
                            c                 = self.c,
                            dbh               = self.dbh,
                            baselinePrecision = self.baselineParameters)
        
            # Does the baseline document need updating?
            if base.needUpdate == True:
                base.processBaseline(self.blankData)
  
