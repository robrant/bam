import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx) 

import unittest
import datetime
import json

from baseObjects import vastTweet

class TestVastTweet(unittest.TestCase):


    def testFinHashTags(self):
        ''' Test the finding of hashtags in text.'''

        text = "This is a #Hashtag #tesT-link #A should#not#work and #this_link"
        vt = vastTweet()
        hts = vt.findHashTags(text)
        print "normal", hts
        truth = ['hashtag', 'test', 'a', 'this_link']
        self.assertEqual(hts, truth)

 
    def testFinHashTagsAdvancedSubject(self):
        ''' Test the finding of hashtags in text.'''

        text = "This is a #Hashtag #test-link #A should#not#work"
        vt = vastTweet()
        hts = vt.findHashTagsAdvanced(text)
        truth = ['hashtag', 'test', 'a']
        print 'Advanced', hts

    def testFinHashTagsAdvancedUser(self):
        ''' Test the finding of hashtags in text.'''

        text = "@brantinr, @helloworld - this is a #Hashtag #test-link #A should#not#work"
        vt = vastTweet()
        hts = vt.findHashTagsAdvanced(text, user=True)
        truth = ['brantinr', 'helloworld']
        print 'Advanced', hts
  

    def testImportData(self):
        ''' Check the __init__ without json object'''
        
        ts = datetime.datetime(2011,1,1,12,1,1)
        lat, lon = 34.4, 45.5
        text = 'Hello world again #Hashtag1 #hashtag2 #hashtag3.'
        tId = 346664
        uId = 4444
        vt = vastTweet()
        vt.importData(timeStamp=ts, lat=lat, lon=lon, text=text, userId=uId, tweetId=tId) 

        self.assertEquals(vt.timeStamp, ts)
        self.assertEquals(vt.lat, lat)
        self.assertEquals(vt.lon, lon)
        self.assertEquals(vt.hashTags, ['hashtag1','hashtag2','hashtag3'])
        self.assertEquals(vt.userId, uId)
        self.assertEquals(vt.tweetId, tId)



    def testImportDataJson(self):
        ''' Check the __init__ WITH A JSON object'''

        d = {'timestamp': datetime.datetime(2011,1,1,12,1,1).strftime('%Y-%m-%dT%H:%M:%S'),
             'lat'      : 34.4,
             'lon'      : 45.5,
             'text'     : 'Hello world again #Hashtag1 #hashtag2 #hashtag3.',
             'tweet_id' : 346664,
             'user_id'  : 4444,
             'hashtags' : None,
             }
                
        jTweet = json.dumps(d)
        
        vt = vastTweet()
        vt.importData(jTweet=jTweet)

        self.assertEquals(vt.timeStamp, datetime.datetime.strptime(d['timestamp'], '%Y-%m-%dT%H:%M:%S'))
        self.assertEquals(vt.lat, d['lat'])
        self.assertEquals(vt.lon, d['lon'])
        self.assertEquals(vt.hashTags, ['hashtag1','hashtag2','hashtag3'])
        self.assertEquals(vt.userId, d['user_id'])
        self.assertEquals(vt.tweetId, d['tweet_id'])
    

    def testvastTweet2Json(self):
        ''' Check the conversion to a json object: '''


        d = {'timestamp': datetime.datetime(2011,1,1,12,1,1).strftime('%Y-%m-%dT%H:%M:%S'),
             'lat'      : 34.4,
             'lon'      : 45.5,
             'text'     : 'Hello world again #Hashtag1 #hashtag2 #hashtag3.',
             'tweet_id' : 346664,
             'user_id'  : 4444,
             'hashtags' : None
             }
        
        jTweet = json.dumps(d)
        vt = vastTweet()
        vt.importData(jTweet=jTweet)
        
        dumped = json.loads(vt.vastTweet2Json())
        
        # Reassign hashtags, now they've been generated
        d['hashtags'] = ['hashtag1', 'hashtag2', 'hashtag3']
        
        for key in d.keys():
            print key, d[key], dumped[key]
            self.assertEquals(d[key], dumped[key])


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()