import fcntl
import logging

from ConnectionManager import ConnectionManager
from DBRequest import DBRequest, QueryRequest

OUTGOING_QUEUE_IP = 'localhost'
OUTGOING_QUEUE_PORT = 5672
OUTGOING_EXCHANGE_NAME = 'buzz-exchange'
OUTGOING_QUEUE_NAME = 'buzz-queue'

INDEX_PATH = './index'

'''Saves the index to locate users and hashtags in messages'''
class DBIndexManager:

    def __init__(self,keyLength):
        logging.getLogger(self.__class__.__name__)
        logging.basicConfig(filename="app.log",format='%(levelname)s:%(asctime)s:%(module)s@%(lineno)d:%(message)s', level=logging.INFO)
        self.keyLength = keyLength

    def storeIndex(self,buzz):
        username = buzz.user
        self.updateIndex(username,buzz.uId)
        hashtags = buzz.getHashtags()
        for hashtag in hashtags:
            self.updateIndex(hashtag,buzz.uId)

    def getFileKey(self,tag):
        if (len(tag) < self.keyLength):
            fileKey = tag
        else:
            fileKey = tag[:self.keyLength]
        return fileKey

    def getTagPosition(self,filelines,tag):
        position = 0
        for line in filelines:
            args = line.split(';')
            if(args[0] == tag):
                return position
            position+=1
        return -1

    def getFileLines(self,filename):
        lines = []
        try:
            file = open(filename,'r')
            fcntl.flock(file,fcntl.LOCK_SH)
            lines = file.readlines()
            fcntl.flock(file,fcntl.LOCK_UN)
            file.close()
        except Exception as e:
            logging.warn(e)
        return [line.rstrip() for line in lines]

    def writeLines(self,filename,lines):
        try:
            file = open(filename, 'w')
            fcntl.flock(file, fcntl.LOCK_EX)
            file.write('\n'.join(lines))
            fcntl.flock(file, fcntl.LOCK_UN)
            file.close()
        except Exception as e:
            logging.warn(e)

    def updateIndex(self,tag,uId):
        uId = str(uId)
        filename = INDEX_PATH + "/" + self.getFileKey(tag)
        filelines = []
        try:
            filelines = self.getFileLines(filename)
        except IOError as e:
            logging.warn(e)
        position = self.getTagPosition(filelines, tag)
        if(position >= 0):
            filelines[position] = filelines[position] + ";" + uId
        else:
            filelines.append(tag + ";" + uId)
        self.writeLines(filename,filelines)

    def getIdList(self,tag):
        filename = INDEX_PATH + "/" + self.getFileKey(tag)
        args = []
        try:
            file = open(filename, 'r')
            fcntl.flock(file,fcntl.LOCK_SH)
            for line in file:
                args = line.split(";")
                if(args[0] == tag):
                    break
            fcntl.flock(file,fcntl.LOCK_UN)
            file.close()
            return [line.rstrip() for line in args[1:]]
        except IOError as e:
            logging.warn(e)
            return []


    def processRequest(self,request):
        logging.info("processing " + request.tag)
        ids = self.getIdList(request.tag)
        outgoingConnectionManager = ConnectionManager(OUTGOING_QUEUE_IP, OUTGOING_QUEUE_PORT)
        outgoingConnectionManager.declareExchange(OUTGOING_EXCHANGE_NAME)
        for id in ids:
            logging.info("Sending id to DB")
            outgoingConnectionManager.writeToExchange(OUTGOING_EXCHANGE_NAME,id,QueryRequest(request.user,id))
        outgoingConnectionManager.close()






