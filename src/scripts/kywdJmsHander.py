#!/usr/bin/python
import time
import sys
import logging
import socket
import stomp
import ConfigParser
import os
import random

from jmsKeywordListeners import keywordListener

# Called using:
#./stomp_listen.py /queue/test

# the stomp module uses logging so to stop it complaining
# we initialise the logger to log to the console
logging.basicConfig()

# first argument is the que path
queue = sys.argv[1]

#------------------------------------------------------------------------------------------ 

def getConfig(path, file):
    ''' Reads in a list of keywords from a config file. '''
    
    config = ConfigParser.ConfigParser()
    try:
        config.read(os.path.join(path, file))
    except:
        print "Failed to read the config file for keyword lookup."
        hosts = None
        
    # Misc parameters
    host = config.get("SectionOne", "host")
    port = int(config.get("SectionOne", "port"))
    hosts = [(host, port)] 
    
    return hosts

#------------------------------------------------------------------------------------------ 
def run_server():
    ''' We want the script to keep running '''

    while 1:
        time.sleep(20)

#------------------------------------------------------------------------------------------ 

# do we have a connection to the server?
connected = False

path = "/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/config/"
file = "jmsConfig.cfg"
hosts = getConfig(path, file)
listenerName = 'listener_%s' %random.randint(0,10000)

while not connected:
    # try and connect to the stomp server
    # sometimes this takes a few goes so we try until we succeed
    try:
        conn = stomp.Connection(host_and_ports=hosts)
        # register out event hander above
        conn.set_listener(listenerName, keywordListener())
        conn.start()
        conn.connect()
        # subscribe to the names que
        conn.subscribe(destination=queue, ack='auto')
        connected = True
    except socket.error:
        pass
    
# we have a connection so keep the script running
if connected:
    run_server()