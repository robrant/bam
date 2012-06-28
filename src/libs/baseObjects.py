import sys

importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx) 

import Geographic
import datetime
import json
import re

import mdb
import mgrs as mgrsLib

class validation(object):
    
    def __init__(self):
        ''' A couple of functions used in base classes   '''
    
    def validateTime(self, timeStamp):
        ''' Simple checks for valid incoming timestamp. - tests written. '''

        # Check that its an instance of timeStamp
        if isinstance(timeStamp, datetime.datetime):
            return timeStamp
        else:
            return None

    #------------------------------------------------------------------------------------------------

    def validateText(self, text):
        '''Validate the encoding of the string content itself.'''
        
        try:
            text = text.decode('latin-1')
        except UnicodeEncodeError, e:
            text = text
        return text
        
    #------------------------------------------------------------------------------------------------

    def validateGeos(self, lon, lat):
        ''' Simple checks for correct lat and lon range. Can I use 'assert' in here?
             Tests written.'''
        
        # Make sure they're not None
        if not lat and not lon:
            print "You must provide a valid lat and lon."
            return None, None

        # Ensure they're float type        
        try:
            lat = float(lat)
            lon = float(lon)
        except:
            print "lat and lon must be numeric type."
            return None, None
            
        # Check their ranges
        if lat > 90.0 or lat < -90.0:
            print "Invalid lat range: %s" %(lat)
            return None, None
        
        # Check their ranges
        if lon > 180.0 or lon < -180.0:
            print "Invalid lon range: %s" %(lon)
            return None, None
        
        return lon, lat
 
 
################################################################################################
################################################################################################


class processEvent():

    def __init__(self, event, source, mgrsPrecision):
        ''' Takes a single argument: a single event object - see module baseObjects'''
        
        # The event object being passed into this class
        self.ev = event
        self.source = source
        self.mgrsPrecision = mgrsPrecision

        # The list of keywords to be returned following processing
        self.keywords = []
        
#-----------------------------------------------------------------------------------------

    def buildKeywordObject(self, token):
        ''' Builds the keyword base objects from the functions'''
        
        kw = keyword(keyword   = str(token),
                     timeStamp = self.ev.timeStamp,
                     lat       = self.ev.lat,
                     lon       = self.ev.lon,
                     text      = self.ev.text,
                     eventId   = self.ev.eventId,
                     userId    = self.ev.userId,
                     source    = self.source,
                     mgrsPrecision=self.mgrsPrecision)
                                    
        self.keywords.append(kw)

#-----------------------------------------------------------------------------------------

    def fromHashTag(self):
        '''Builds a keyword object based on the hashtag list in the incoming event object'''
        
        # Bail if no hashtags
        if not self.ev.hashTags:
            return None
        
        # For each hashtag, build a keyword object
        for ht in self.ev.hashTags:    
            self.buildKeywordObject(ht)
        else:
            return None
                    
#-----------------------------------------------------------------------------------------

    def fromLookup(self, lookup):
        ''' Builds new keywords based on a keyword gazetteer.
            Needs massive improvement - aliases, partial matching, spelling difs. ''' 
        
        # Splits the text up into words
        regExp = re.compile(r'(\b\w+\b)')
        regOut = re.findall(regExp, self.ev.text) 
        
        if not regOut:
            return None
        
        # If the word is in the LOOKUP (provided up top), but not in the hashtag list (ie. duped)
        # then add it to the list of keywords to be processed.
        for token in regOut:
            if self.ev.hashTags and token.lower() in self.ev.hashTags:
                continue 
            elif token.lower() in lookup:
                self.buildKeywordObject(token)

#-----------------------------------------------------------------------------------------

    def whenNothingFound(self, tokenName = '--nothingfound--'):
        ''' When nothing is found in the event content, rather than lose the observation,
            this function builds a '--nothingFound--' keyword object'''
        
        self.buildKeywordObject(tokenName)
        
#-----------------------------------------------------------------------------------------

    def fromNLP(self):
        ''' Uses NLP to identify entities. 
            Perhaps combining LOOKUP, NLP and Geo searches with something like NLTK:
            http://www.packtpub.com/python-text-processing-nltk-20-cookbook/book''' 

#-----------------------------------------------------------------------------------------

    def fromFullMessage(self, path, fName):
        ''' Process every significant word into a keyword object to process. ''' 

################################################################################################
################################################################################################

class eventRecord(validation):
    
    
    def __init__(self, source, record):
        '''Constructor only'''

        self.source         = source
        self.timeStamp      = self.checkTimeStamp(record)
        lon, lat            = self.getGeos(record)
        self.lon, self.lat  = self.validateGeos(lon, lat)
        self.text           = self.validateText(record['text'])
        self.hashTags       = self.checkHashtags(record)
        self.eventId        = self.checkRecordId(record)
        self.userId         = self.checkUserId(record)
        self.platform       = self.sourcePlatform(record)
         
    #-------------------------------------------------------------------------------------------------

    def sourcePlatform(self, record):
        ''' Attempts to find the platform type from which the record was raised.
            Was it for example a mobile tweet?'''
        
        if record.has_key('source'):
            platform = record['source']
            st = platform.find('>')
            sp = platform.find('<', st)
            
            # For platform/source held in a url <a> tag
            if st > -1 and sp > -1:
                platform = platform[st+1:sp]
        else:
            platform = None
            
        return platform
        
    #-------------------------------------------------------------------------------------------------

    def checkTimeStamp(self, record):
        ''' Check for the timestamp field in the record.'''

        try:
            ts = record['created_at']
        except KeyError, e:
            print e
            ts = None

        # Format for tweets: Sat May 26 21:23:41 +0000 2012
        try:
            ts = datetime.datetime.strptime(ts, '%a %b %d %H:%M:%S +0000 %Y')
        except ValueError, e:
            print 'Failed to parse the record timestamp.'
            print e
            ts = None
            
        return ts
    #-------------------------------------------------------------------------------------------------

    def checkUserId(self, record):
        ''' Check for the ID of the user.'''

        if record.has_key('user'):

            if record['user'].has_key('id'):
                userId = record['id']
    
            else:
                print 'could not find a tweet ID field'
                userId = None
        else:
                print 'could not find a User ID field.'
                userId = None
        
        return userId

    #-------------------------------------------------------------------------------------------------

    def checkRecordId(self, record):
        ''' Check for the ID of the tweet.'''

        if record.has_key('id'):
            recordId = record['id']

        else:
            print 'could not find an ID field'
            recordId = None
        
        return recordId

    #-------------------------------------------------------------------------------------------------

    def checkHashtags(self, record):
        ''' Check for hashtags in the input.'''

        hts = []
        
        # Check the input record has a hashtags key and it has some content
        if record.has_key('hashtags') and len(record['hashtags']) > 0:
            for ht in record['hashtags']:
                hts.append(ht['text'])
        # Else do a regex search for hashtags - more relevant for inputs that haven't already broken them out.
        else:
            hts = self.findHashTagsAdvanced(record['text'])
        
        return hts
        
    #-------------------------------------------------------------------------------------------------

    def getGeos(self, record):
        ''' Extract the lat and lon'''

        if record.has_key('coordinates') == True:
            if record['coordinates']['type']:
                lon, lat = record['coordinates']['coordinates']
            else:
                print 'not point type geo.'
        else:
            lon, lat = None, None
            
        return lon, lat

#------------------------------------------------------------------------------------------ 

    def findHashTagsAdvanced(self, text, user=None):
        ''' Function to extract hashtags from a string of text.
            This will not pick out those with punctuation - '_', '-', etc.
            From: http://stackoverflow.com/questions/2527892/parsing-a-tweet-to-extract-hashtags-into-an-array-in-python
            Tests written.'''
        
        if user: tag = '@'
        else:    tag = '#'
        
        # This regex returns a triple for each match: (space, #, hashtag).
        UTF_CHARS = ur'a-z0-9_\u00c0-\u00d6\u00d8-\u00f6\u00f8-\u00ff'
        TAG_EXP = ur'(^|[^0-9A-Z&/]+)(%s|\uff03)([0-9A-Z_]*[A-Z_]+[%s]*)' %(tag, UTF_CHARS)
        x = re.compile(TAG_EXP, re.UNICODE | re.IGNORECASE)        
        hts = x.findall(text)
        
        htsOut = list(ht[-1].lower() for ht in hts)
        if len(htsOut) > 0:
            return htsOut
        else:
            return None

       
################################################################################################
################################################################################################


class eventRecordVast(validation):
    
    
    def __init__(self):
        '''Constructor only'''

    #-------------------------------------------------------------------------------------------------
    
    def importData(self, eventId=None, timeStamp=None, lat=None, lon=None, text=None, userId=None):
        
        ''' Object to hold an event (tweet, photo metadata, etc. Has one function built in for 
            finding hashtags - primarily for twitter. Others could be added for standard types.'''

        self.timeStamp     = self.validateTime(timeStamp)
        self.lat, self.lon = self.validateGeos(lon, lat)
        self.text          = self.validateText(text)
        self.hashTags      = self.findHashTagsAdvanced(text)
        self.eventId       = eventId
        self.userId        = userId
        
#------------------------------------------------------------------------------------------ 

    def findHashTagsAdvanced(self, text, user=None):
        ''' Function to extract hashtags from a string of text.
            This will not pick out those with punctuation - '_', '-', etc.
            From: http://stackoverflow.com/questions/2527892/parsing-a-tweet-to-extract-hashtags-into-an-array-in-python
            Tests written.'''
        
        if user: tag = '@'
        else:    tag = '#'
        
        # This regex returns a triple for each match: (space, #, hashtag).
        UTF_CHARS = ur'a-z0-9_\u00c0-\u00d6\u00d8-\u00f6\u00f8-\u00ff'
        TAG_EXP = ur'(^|[^0-9A-Z&/]+)(%s|\uff03)([0-9A-Z_]*[A-Z_]+[%s]*)' %(tag, UTF_CHARS)
        x = re.compile(TAG_EXP, re.UNICODE | re.IGNORECASE)        
        hts = x.findall(text)
        
        htsOut = list(ht[-1].lower() for ht in hts)
        if len(htsOut) > 0:
            return htsOut
        else:
            return None
        
################################################################################################
################################################################################################

class keyword(validation):
    '''
    A keyword object stores attributes to do with a specific keyword identified
    in a tweet/image/message/post. Where multiple hashtags or keywords have been identified in a
    a tweet, a keyword object is built for each one. 
    
    The intent is that these are placed on a python Queue (or back onto the JMS)
    for processing into density in Mongo.
    
    MGRS is added in here.
    
    '''

    def __init__(self, keyword=None, timeStamp=None, lat=None, lon=None, text=None,
                 eventId=None, userId=None, source=None, mgrsPrecision=6):
        ''' Constructor. Validation of latlon and time are handled by inheriting the validation class.                - '''
        
        # If the class is called without a json object
        self.keyword        = keyword.lower()
        self.text           = text.lower()
        self.userId         = userId
        self.source         = source.lower()
        self.created        = datetime.datetime.utcnow()
        self.eventId        = eventId
        self.mgrsPrecision  = mgrsPrecision
        
        #Handle and validate the timestamp info
        self.timeStamp = self.validateTime(timeStamp)

        # Handle and validate the Geos
        self.lon, self.lat  = self.validateGeos(lon,lat)        
        if lat and lon:
            m = mgrsLib.MGRS()
            self.mgrs  = m.toMGRS(self.lat, self.lon, MGRSPrecision=mgrsPrecision/2)
        else:
            self.mgrs  = None    

    #------------------------------------------------------------------------------------------------
        
    def getMGRS(self, precision=10):
        ''' Converts the geos to MGRS. The optional precision determines the number of figures for
            the MGRS conversion.
            Tests written.'''
        
        utm  = Geographic.geo2Utm(self.lat, self.lon)
        mgrs = Geographic.utm2Mgrs(utm[0], utm[1], utm[2], precision)
        
        return mgrs

#------------------------------------------------------------------------------------------ 

    def keyword2Json(self):
        ''' Converts the keyword object to json. Tests written.'''
        
        d = {'timestamp': self.timeStamp.strftime('%Y-%m-%dT%H:%M:%S'),
             'created':   self.created.strftime('%Y-%m-%dT%H:%M:%S'),
             
             'lat':       self.lat,
             'lon':       self.lon,
             'mgrs':      self.mgrs,
             
             'text':      self.text,
             'source':    self.source,
             'keyword':  self.keyword,
             'eventId':  self.eventId,
             'userId':   self.userId
             }

        return json.dumps(d)

#------------------------------------------------------------------------------------------ 

    def buildKeywordForMongo(self):
        ''' Function to format keyword into document for mongo.
            This would be used if map/reduce was going to be used
            for generating count statistics per cell.'''        
        
        doc = {'mgrs'           : self.mgrs,
               'mgrs_precision' : self.mgrsPrecision,
               'keyword'        : self.keyword,
               'source'         : self.source,
               'timestamp'      : self.timeStamp,
               'userId'        : self.userId,
               'eventId'       : self.eventId
               }
        
        try:
            self.dbh.keywords.insert(doc)
            return 1
        except:
            print "Failed to insert object into mongo 'timeseries' collection."
            return None


#------------------------------------------------------------------------------------------------
        
    def getGeoGazetteerLookup(self):
        ''' Searches the text string for place names that could be geolocated.'''
        
        '''
        
        Only necessary where there isn't a geo...
        
        Tube stations - tuffnell park
        Boroughs - 
        Major London centres - westminster, oxford street.
        Buildings and landmarks - parliament, big ben, trafalgar square,
        Different precisions associated with these?
        
        **** THIS IS SOMETHING THAT COULD BE DONE ONLINE? ****
        or possibly a combination - quick check against a local gaz
        and then accurate coords retrieved from OS?
        
        Can possibly use wikipedia and infochimps for this...
        
        '''
