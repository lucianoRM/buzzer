import fcntl

from Buzz import Buzz
from ConnectionManager import ConnectionManager
from DBRequest import DBRequest

OUTGOING_QUEUE_IP = 'localhost'
OUTGOING_QUEUE_PORT = 5672
OUTGOING_QUEUE_NAME = 'dbaccess-queue'

INDEX_PATH = './index'

'''Saves the index to locate users and hashtags in messages'''
class DBIndexManager:

    def __init__(self,keyLength):
        self.keyLength = keyLength
        self.outgoingConnectionManager = ConnectionManager(OUTGOING_QUEUE_IP,OUTGOING_QUEUE_PORT)

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
        file = open(filename,'r')
        fcntl.flock(file,fcntl.LOCK_SH)
        lines = file.readlines()
        fcntl.flock(file,fcntl.LOCK_UN)
        file.close()
        return [line.rstrip() for line in lines]

    def writeLines(self,filename,lines):
        file = open(filename, 'w')
        fcntl.flock(file, fcntl.LOCK_EX)
        file.write('\n'.join(lines))
        fcntl.flock(file, fcntl.LOCK_UN)
        file.close()

    def updateIndex(self,tag,uId):
        uId = str(uId)
        filename = INDEX_PATH + "/" + self.getFileKey(tag)
        filelines = []
        try:
            filelines = self.getFileLines(filename)
        except IOError as e:
            print e
        position = self.getTagPosition(filelines, tag)
        if(position >= 0):
            filelines[position] = filelines[position] + ";" + uId
            print filelines
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
            print e
            return []


    def processRequest(self,request):
        ids = self.getIdList(request.tag)
        print ids
        #for id in ids:
         #   self.outgoingConnectionManager.writeToQueue(OUTGOING_QUEUE_NAME,DBRequest(request.user,id))





