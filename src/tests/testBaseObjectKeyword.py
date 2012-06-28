'''
Created on Feb 7, 2012

@author: brantinghamr
'''
import unittest
import datetime
from baseObjects import keyword
import json

class TestKeyword(unittest.TestCase):


    def setUp(self):
        ''' Builds the object in this case '''
        
        # For timing purposes - the start of build
        self.startBuild = datetime.datetime.utcnow()
        
        keyword1='helloWorLD'
        timeStamp=datetime.datetime(year=2011,month=1,day=2,hour=10,minute=30,second=45)
        lat = 45.5
        lon = 34.4  
        text="The strangest thing happened today. Someone said #helloWorLD to ME."
        tweetID=10450066
        userID='brantinr'   # Need to check whether this is a string or number
        source='TWITTER'
        
        # Build a keyword object (keyword 1)
        self.kw = keyword(keyword1, timeStamp, lat, lon, text, tweetID, userID, source)
        
    def testLat(self):
        ''' Valid Lat Attribute? '''
        self.assertAlmostEquals(self.kw.lat, 45.5, 4)

    def testLon(self):
        ''' Valid Lon Attribute? '''
        self.assertAlmostEquals(self.kw.lon, 34.4, 4)
        
    def testMGRS(self):
        ''' Valid MGRS output?'''
        self.assertEquals(self.kw.mgrs, "36TXR0937739450")

    def testTimeStamp(self):
        ''' Valid timestamp output?'''
        truth = datetime.datetime(year=2011,month=1,day=2,hour=10,minute=30,second=45)
        self.assertEquals(self.kw.timeStamp, truth)
        
    def testText(self):
        ''' Valid text output?'''        
        self.assertEquals(self.kw.text, "the strangest thing happened today. someone said #helloworld to me.")

    def testTweetID(self):
        ''' Valid tweetID output?'''        
        self.assertEquals(self.kw.tweetID, 10450066)
        
    def testUserID(self):
        ''' Valid userID output?'''        
        self.assertEquals(self.kw.userID, 'brantinr')
        
    def testSource(self):
        ''' Valid source output?'''        
        self.assertEquals(self.kw.source, 'twitter')

    def testvastTweet2Json(self):
        ''' Check the conversion to a json object: '''
        
        keyword1='helloWorLD'
        timeStamp=datetime.datetime(year=2011,month=1,day=2,hour=10,minute=30,second=45)
        lat = 45.5
        lon = 34.4  
        text="The strangest thing happened today. Someone said #helloWorLD to ME."
        tweetID=10450066
        userID='brantinr'   # Need to check whether this is a string or number
        source='TWITTER'
        
        # Build a keyword object (keyword 1)
        self.kw = keyword(keyword1, timeStamp, lat, lon, text, tweetID, userID, source)
        
        dumped = json.loads(self.kw.keyword2Json())
        self.assertEqual(dumped['user_id'], 'brantinr')
        self.assertEqual(dumped['keyword'], 'helloworld')
        self.assertEqual(dumped['mgrs'], '36TXR0937739450')

    def tearDown(self):

        # Build time for seeing how long it takes to build an object
        self.endBuild = datetime.datetime.utcnow()
        
        print "="*40; print "="*40
        print "Object build and testing took: %s" %(self.endBuild - self.startBuild)
        print self.kw.__sizeof__()
        print "="*40; print "="*40
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testTweet2Tag']
    unittest.main()