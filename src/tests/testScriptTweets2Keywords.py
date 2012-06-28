import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx)

import unittest
import datetime
# Need the base objects for testing
from baseObjects import vastTweet, keyword

# The class/fns being tested
from scriptTweets2Keywords import processTweet

#========================================================================================

class TestScriptTweets2Keywords(unittest.TestCase):


    def setUp(self):
        ''' Setup an object to work with '''
        
        # Build a tweet
        ts = datetime.datetime(2011,1,1,12,1,1)
        lat, lon = 34.4, 45.5
        text = 'Hello germ world again #Hashtag1 #hashtag2 #hashtag3 virus.'
        tId = 346664
        uId = 4444
        
        self.vt = vastTweet()
        self.vt.importData(timeStamp=ts, lat=lat, lon=lon, text=text, userId=uId, tweetId=tId) 


#-----------------------------------------------------------------------------------------

    def testBuildKeywordObject(self):
        ''' Just checks the ability build a keyword object with a token/keyword/hashtag '''

        twt = processTweet(self.vt)
        twt.buildKeywordObject('hello_world')
        
        # Check we see 3 objects in the list
        self.assertEquals(len(twt.keywords), 1)

        # Check one of those has a valid MGRS
        self.assertEquals(twt.keywords[0].mgrs, '38SND4595706622')

#-----------------------------------------------------------------------------------------

    def testFromHashTag(self):
        ''' Checks getting a list of keywords from the tweets keyword attr'''
        
        # Process to keywords
        twt = processTweet(self.vt)
        twt.fromHashTag()
        
        # Check we see 3 objects in the list
        self.assertEquals(len(twt.keywords), 3)

        # Check one of those has a valid MGRS
        self.assertEquals(twt.keywords[0].mgrs, '38SND4595706622')

#-----------------------------------------------------------------------------------------

    def testFromGazetteer(self):
        ''' Check keywords based on a gazetteer lookup.
            Initially this could just be python dict, but it could be
            extended to a SQLite or Mongo collection of names/places. '''


        twt = processTweet(self.vt)
        twt.fromLookup()
        
        print twt.keywords
        print len(twt.keywords)
        # Simple length check based on content in the tweet
        self.assertEquals(len(twt.keywords), 2)
        
        # Check one of the keywords
        self.assertEquals(twt.keywords[0].keyword, 'germ')
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test']
    unittest.main()