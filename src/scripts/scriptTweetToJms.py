'''

Script file to put tweets onto a JMS topic

1. 
'''

import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx)

# Standard Libs
import datetime
import os
import time
import json
import copy

# Custom libs
import jmsCode                      # JMS STOMP connection wrapper - needs stomp.py

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
            print "ID number %s doesn't have valid geos: %s" %(tId, location)
            lat, lon = None, None
            
    else:
        print "ID number %s doesn't have valid geos: %s" %(tId, location)
        lat, lon = None, None
    
    return [lat, lon]


#------------------------------------------------------------------------------------------ 

def getTime(tId, dt):
    ''' Check the timestring is valid. Tests Written. '''

    # Get the time info (#5/20/2011 16:55)
    try:
        dtg = datetime.datetime.strptime(dt, '%m/%d/%Y %H:%M')
    except ValueError:
        dtg = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
    except:
        dtg = None
        
    return dtg

#------------------------------------------------------------------------------------------ 

def main(): 
    ''' Puts json representations of tweets onto a JMS for later processing into keywords, etc.
        Currently configured to work with the VAST dataset rather than a live tweet stream. '''

    startTimer = datetime.datetime.utcnow()
    lastTimer = datetime.datetime.utcnow()
    
    # JMS PARAMETERS
    destination = '/queue/test.tweets'
    hostIn      = 'localhost'
    portIn      = 61613
    # File Path Info
    path       = "/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/data/"
    configPath = "/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/config/"
    fName= "MicroblogsOrdered.csv"
    # Read in a twitter template
    templateName = 'recordTemplate.json'
    templateFile = open(os.path.join(configPath, templateName))
    template = json.loads(templateFile.read())
    
    # Make the JMS connection via STOMP and the jmsCode class    
    jms = jmsCode.jmsHandler(hostIn, portIn, verbose=True)
    jms.connect()

    f = retrieveFile(path, fName)
    x = 0
    
    # Loop the lines build tweet objects
    for line in f.readlines():
        
        # Extract content from each line
        line = line.rstrip('\r').rstrip('\n').rstrip('\r')

        if x == 0:
            x+=1
            continue
        
        if x % 2000 == 0:
            print "Processed: ", x
            counterTimer = datetime.datetime.utcnow()
            intermediateTimer = counterTimer - lastTimer
            lastTimer = datetime.datetime.utcnow()
            print "2,000 taken: %s" %intermediateTimer
        
        if x % 500000 == 0:
            jms.disConnect()
            f.close()
            sys.exit()
            
        line = line.split(',')
        tweetId, dt, latLon, text = line
        
        # Get the geos
        geos = getGeos(tweetId, latLon)
        if not geos[0]: continue
        
        # Get the datetime group into seconds since UNIX time
        dtg = getTime(tweetId, dt)
        if not dtg: continue
        
        tweetJson = copy.copy(template)
        tweetJson['app'] = 'twitter'
        tweetJson['user']['id'] = tweetId
        tweetJson['coordinates']['coordinates'] = [geos[1], geos[0]]
        tweetJson['created_at'] = dtg.strftime('%a %b %d %H:%M:%S +0000 %Y')
        tweetJson['text'] = text
        
        #==============================================================================
        # Could put in here something to add in a platform (mobile vs static, etc)
        #==============================================================================
        
        try:
            tweetJson = json.dumps(tweetJson)
        except:
            tweetJson['text'] = text.decode('latin-1')
            tweetJson = json.dumps(tweetJson)
        
        jms.sendData(destination, tweetJson, x)
    
        #time.sleep(10)
        x+=1
        
    # Disconnect from the topic    
    jms.disConnect()
    f.close()

    stopTimer = datetime.datetime.utcnow()
    takenTimer = stopTimer - startTimer
    print "TimeTaken: %s" %takenTimer

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    
    import profile
    profile.run('main()')
    