import fcntl

from Buzz import Buzz
from ConnectionManager import ConnectionManager

ROOT_PATH = './buzz'
OUTGOING_CONNECTION_IP = 'localhost'
OUTGOING_CONNECTION_PORT = 5672



'''Handles db files and stores information'''
class DBBuzzManager:


    def __init__(self,keyLength):
        self.keyLength = keyLength


    def createInfoEntry(self,buzz):
        argsList = []
        argsList.append('+') # + means that the buzz is accesible
        argsList.append(str(buzz.uId))
        argsList.append(buzz.user)
        argsList.append(buzz.message)
        return ";".join(argsList)

    def getFileKey(self,uId):
        fileKey = uId[:self.keyLength]
        return fileKey

    def getFilePath(self, uId):
        return ROOT_PATH + "/" + self.getFileKey(uId)

    def store(self,buzz):
        filename = self.getFilePath(str(buzz.uId))
        file = open(filename, 'a+')
        fcntl.flock(file,fcntl.LOCK_EX)
        file.write('\n' + self.createInfoEntry(buzz))
        fcntl.flock(file, fcntl.LOCK_UN)
        file.close()

    def retrieveBuzz(self, uId):
        uId = str(uId)
        filename = self.getFilePath(uId)
        file = open(filename, 'r')
        buzz = None
        fcntl.flock(file, fcntl.LOCK_SH)
        for line in file:
            args = line.split(";")
            if(args[0] == "+" and args[1] == uId):
                buzz = ";".join(args[1:])
                break
        fcntl.flock(file, fcntl.LOCK_UN)
        file.close()
        return self.CSVToBuzz(buzz)

    def deleteBuzz(self, requestObject):
        uId = str(requestObject.uId)
        filename = self.getFilePath(uId)
        file = open(filename, 'r+')
        fcntl.flock(file, fcntl.LOCK_EX)
        location = 0
        found = False
        for line in file:
            args = line.split(";")
            if (args[0] == "+" and args[1] == uId):
                found = True
                break
            location+=len(line)
        if(found):
            file.seek(location)
            file.write("-")
        fcntl.flock(file, fcntl.LOCK_UN)
        file.close()

    def CSVToBuzz(self,csvLine):
        if(not csvLine):
            return None
        args = csvLine.split(";") #id;user;message
        return Buzz(args[1],args[2].rstrip(),args[0])

    def processRequest(self, requestObject):
        uId = str(requestObject.tag)
        b = self.retrieveBuzz(uId)
        if(b):
            outgoingConnectionManager = ConnectionManager(OUTGOING_CONNECTION_IP, OUTGOING_CONNECTION_PORT)
            outgoingConnectionManager.declareQueue(requestObject.user)
            outgoingConnectionManager.writeToQueue(requestObject.user, b)
            outgoingConnectionManager.close()










