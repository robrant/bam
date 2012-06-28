import subprocess
import mdb

def callBaseliner(scriptFile, host, port, db, kwObj, baselineParameters, mac=None):
    ''' Kicks of a command line operation and doesn't wait for the response.
        The script it calls is provided as an argument. 
        The function is calledby jmsKeywordListeners.py. '''
    
    # Mac has a weird 32-bit vs 64-bit thing that needs this line to run in 32-bit mode on a 32 bit machine
    if mac:
        pythonVersion = 'arch -i386 /usr/bin/python2.6'
    else:
        pythonVersion = 'python'
        
    # Kick off the baseline processing via subprocess
    cmds = [pythonVersion, scriptFile,
            '-H', host,
            '-p', str(port),
            '-d', db,
            '-m', kwObj.mgrs,
            '-M', str(kwObj.mgrsPrecision),
            '-t', kwObj.timeStamp.strftime("%Y-%m-%dT%H:%M:%S"),
            '-k', str(kwObj.keyword),
            '-u', baselineParameters[0],
            '-v', str(baselineParameters[1])]
    
    command = ' '.join(cmds)
    print command
    
    call = subprocess.Popen(command, shell=True)

    return call
        