import sys

importDir = ['../tests/', '../scripts/', '../libs/', ]
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx)

import unittest
import jmsCode

class TestJmsHandler(unittest.TestCase):
    
    def setUp(self):
        ''' instantiate objects'''

        self.hostIn  = 'localhost'
        self.portIn       = 61613

        self.jms = jmsCode.jmsHandler(self.hostIn, self.portIn, verbose=True)


    def testConnect(self):
        ''' Just check the connection: '''
        
        self.jms.connect()
        connected = self.jms.conn.is_connected()
        self.assertEquals(connected, True)
        
        h,p = self.jms.conn.get_host_and_port()
        self.assertEquals(h, self.hostIn)
        self.assertEquals(p, self.portIn)
        
        self.jms.conn.disconnect()
        
    def testReConnect(self):
        ''' Check the disconnection: '''
        
        self.jms.connect()
        connected = self.jms.conn.is_connected()
        self.assertEquals(connected, True)
        
        self.jms.reConnect()
        connected = self.jms.conn.is_connected()
        self.assertEquals(connected, True)
        
        self.jms.disConnect()


    def receiveData(self):
        ''' Test the reception of data: '''
        
        self.jms.connect()
        
        self.jms.receiveData(dest='/topic/test.t.helloworld')
        
        
    def testSendData(self):
        ''' Test the sending of data: '''

        message = 'helloworld Apache'
        msgId   = 111
        dest    = '/topic/test.t.helloworld'

        self.jms.connect()
        self.jms.sendData(dest, message, msgId)
        
        self.jms.disConnect()
        