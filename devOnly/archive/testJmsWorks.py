import time
import sys

import stomp


class MyListener(object):

    def on_connected(self, headers, body):
        
        self.incoming = ''
    
    def on_error(self, headers, message):
        print 'received an error %s' % message
        
        return message
    
    def on_message(self, headers, message):
      
        self.incoming = message
        print "Incoming", self.incoming
        
        print 'received a message %s' % message
        self.msg = message

conn = stomp.Connection()
conn.set_listener('x', MyListener())
conn.start()
conn.connect()

conn.subscribe(destination='/queue/test', ack='auto')

conn.send('helloworld....', destination='/queue/test')

print 

time.sleep(2)
conn.disconnect()