'''

- This takes json event object and processes it into constituent keyword objects.

- It then loops them and identifies how many keyword objects it needs to build
  based on whether its just using hashtags or using some form of text parsing too.

- For each one it needs to build, it instantiates a new 'keyword' object and adds it to a list
  of keywords for returning from this function.
  

'''
import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx)
import datetime
import re

# Keyword objects
from baseObjects import keyword

#========================================================================================

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
                     eventID   = self.ev.eventId,
                     userID    = self.ev.userId,
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
        
#-----------------------------------------------------------------------------------------
