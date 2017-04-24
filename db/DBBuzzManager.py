import fcntl
import logging

from config import config
from connection.ConnectionManager import ConnectionManager
from messages.Buzz import Buzz





'''Handles db files and stores information'''
class DBBuzzManager:


    def __init__(self,keyLength):
        logging.getLogger(self.__class__.__name__)
        logging.basicConfig(filename="app.log",format='%(levelname)s:%(asctime)s:%(module)s@%(lineno)d:%(message)s', level=logging.INFO)
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
        return config.dbBuzzManagerFilePath() + "/" + self.getFileKey(uId)

    def store(self,buzz):
        logging.info("Storing buzz")
        filename = self.getFilePath(str(buzz.uId))
        try:
            file = open(filename, 'a+')
        except IOError as e:
            logging.error("Folder path not configured for user registration")
        fcntl.flock(file,fcntl.LOCK_EX)
        file.write('\n' + self.createInfoEntry(buzz))
        fcntl.flock(file, fcntl.LOCK_UN)
        file.close()

    def retrieveBuzz(self, uId):
        logging.info("Retrieving buzz")
        uId = str(uId)
        filename = self.getFilePath(uId)
        try:
            file = open(filename, 'r')
        except IOError as e:
            logging.warn(e)
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
        logging.info("Deleting buzz")
        uId = str(requestObject.uId)
        filename = self.getFilePath(uId)
        try:
            file = open(filename, 'r+')
        except IOError as e:
            logging.error(e)
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
        logging.info("Processing request")
        uId = str(requestObject.tag)
        b = self.retrieveBuzz(uId)
        if(b):
            logging.info("Sending to user")
            outgoingConnectionManager = ConnectionManager(config.ip(), config.port())
            outgoingConnectionManager.declareQueue(requestObject.user)
            outgoingConnectionManager.writeToQueue(requestObject.user, b)
            outgoingConnectionManager.close()










