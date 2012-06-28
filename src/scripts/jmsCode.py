import time
import sys
import logging
import socket
import stomp
import random

''' Guidelines:

1. Download apacheMQ JMS
2. Unzip and navigate to the ActiveMQ directory
3. Navigate to conf directory and add
    <transportConnectors>
        <transportConnector name="openwire" uri="tcp://localhost:61616"/>
        <transportConnector name="stomp" uri="stomp://localhost:61613"/>
    </transportConnectors>
to the activemq.xml document

4. Open a cmd prompt and navigate to activeMQ directory. In here type bin/activemq.
5. Firefox/IE navigate to http://localhost:8161/admin
6. demos are in /demos
7. create a topic or a q

On Mac OSX - use the executable in the macosx directory under /bin to start
This starts activemq and activemq-admin nicely without leaving nasty kahadb
locks in place.

Some good help here: https://twiki.cern.ch/twiki/bin/view/EGEE/MsgTutorial
 

'''

#---------------------------------------------------------------------------------

class serverListener(stomp.ConnectionListener):
    ''' Event handler for VAST Tweet data'''
    

    def __init__(self, q=None):
        '''    '''
        
        self.q = q
        

    # If we receive and error from the server
    def on_error(self, headers, message):
        
        print "Received ERROR from the server: %s" %(message)
        
#----------------------------------------------------------------------
        
    # If we receive a message from the server
    def on_message(self, headers, message):
        '''    '''
        
        self.message = message
        self.msgHdrs = headers
        
        
#----------------------------------------------------------------------

    def breakIntoKeywords(self):
        ''' Get data from a topic or Q. '''
            
        self.q.put(self.message)
        

#---------------------------------------------------------------------------------  
    
''' FOR SENDING DATA '''

class jmsHandler():
    
    def __init__(self, host='localhost', port=61613, verbose=None):
        
        ''' Set up the handler'''
    
        self.host = host
        self.port = port
        self.verbose = verbose
        
        # Defaults for local stomp server instance
        self.hostProperties = [(host, port)]
        

#----------------------------------------------------------------------

    def connect(self):
        ''' Attempt connection '''
        
        # Instantiate a connection
        try:
            self.conn = stomp.Connection(self.hostProperties)
            if self.verbose:
                print 'Connection successful'

        except:
            if self.verbose:
                print 'Failed to instantiate a Connection().'
            return None
        
        # Setup the listener
        #try:
        #    self.conn.set_listener('listen', serverListener())
        #except:
        #    if self.verbose:
        #        print "Failed to set the listener on the connection"
        #       return None
        
        # Start the connection
        try:
            self.conn.start()        
            self.conn.connect(wait=True)
            return 1
        except:
            if self.verbose:
                print 'Established a Connection(), but failed to connect.'
                return None
            
            
#----------------------------------------------------------------------
  
    def disConnect(self):
        ''' Close the connection'''
        
        try:
            #print self.conn.is_connected()
            self.conn.stop()
            self.conn.disconnect()
        except Exception, e:
            print e
            pass
  
#----------------------------------------------------------------------
  
    def reConnect(self):
        ''' Close the connection and then reopen it and get a conn attribute.'''
        
        self.disConnect()
        self.connect()

#----------------------------------------------------------------------

    def sendData(self, dest, message, msgId=None):
        ''' Sends data to a Q or topic:
                message = 'testMessage'
                dest = 'topic/test.t.helloworld' '''
                
        # If an id isn't provided, use a randomly assigned one
        if not msgId:
            msgId = random.randint(0,65535)
    
        
        #self.conn.subscribe(destination=dest, ack='auto')
        
        # Sending a message to the JMS
        x = self.conn.send(message=message,
                       destination=dest,
                       headers={'seltype'        : 'mandi-age-to-man',
                                'type'           : 'textMessage',
                                'MessageNumber'  : msgId,
                                'ack'            :'auto'})

        
