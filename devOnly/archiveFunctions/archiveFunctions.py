
#------------------------------------------------------------------------------------------ 

    def findHashTags(self, text):
        ''' Function to extract hashtags from a string of text. This
            one will pick up those cases that have punctuation in them, such
            as '_' or '-'. Tests written.'''
        
        x = re.compile(r'\B#\w+')
        hts = x.findall(text)

        htsOut = list(ht.strip('#').lower() for ht in hts)
        if len(htsOut) > 0:
            return htsOut
        else:
            return None
        
            
#------------------------------------------------------------------------------------------ 

    def vastTweet2Json(self):
        ''' Converts the object to json for JMS.
            Tests written.'''
        
        d = {'timestamp': self.timeStamp.strftime('%Y-%m-%dT%H:%M:%S'),
             'lat':       self.lat,
             'lon':       self.lon,
             'text':      self.text,
             'hashtags':  self.hashTags,
             'tweet_id':  self.tweetId,
             'user_id':   self.userId}

        return json.dumps(d)
    
#-------------------------------------------------------------------------------------------------
    
    def importData(self, jTweet=None, tweetId=None, timeStamp=None, 
                       lat=None, lon=None, text=None, userId=None):
        ''' Object to hold a tweet - no processing, just raw tweet info and metadata.
            If jTweet (a json formatted tweet) exists, builds it from that directly.
            Tests written.'''
        
        if jTweet:
            try:
                # Load from json to python and assign
                jt = json.loads(jTweet)
                
                ts = datetime.datetime.strptime(jt['timestamp'],'%Y-%m-%dT%H:%M:%S')
                
                self.timeStamp     = self.validateTime(ts)
                self.lat, self.lon = self.validateGeos(jt['lat'], jt['lon'])    
                self.text          = jt['text']
                
                try:
                    # If the hashtags == None... or raises a key error
                    if not jt['hashtags']:
                        self.hashTags = self.findHashTagsAdvanced(self.text) 
                    else:
                        self.hashTags = jt['hashtags']
                except KeyError:
                    self.hashTags = self.findHashTagsAdvanced(self.text)
                    
                self.tweetId       = jt['tweet_id']
                self.userId        = jt['user_id']
                
            except:
                print 'Failed to parse JSON into vastTweet Object. '
        else:
            self.timeStamp     = self.validateTime(timeStamp)
            self.lat, self.lon = self.validateGeos(lat, lon)
            self.text          = self.validateText(text)
            self.hashTags      = self.findHashTagsAdvanced(text)
            self.tweetId       = tweetId
            self.userId        = userId