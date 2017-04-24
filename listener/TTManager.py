import collections
import logging

from config import config
from connection.ConnectionManager import ConnectionManager
from db.DBRequest import QueryRequest
from messages.Buzz import Buzz
from messages.TrendingTopic import TTRequest, TTResponse
from listener.GenericListener import GenericListener
from utils.MessageUtils import MessageUtils



class TTManager(GenericListener):

    def __init__(self):
        GenericListener.__init__(self,config.ip(),config.port())
        self.incomingConnectionManager.declareQueue(config.ttQueueName())
        self.hashtagsDicc = {}
        self.ttlist = []
        self.outgoingBuzzDBConnectionManager = ConnectionManager(config.ip(),config.port())
        self.outgoingBuzzDBConnectionManager.declareExchange(config.dbExchange())

    def updateTrendingTopics(self,hashtag,total):
        pos = 0
        for tuple in self.ttlist:
            if (tuple[0] == hashtag):
                self.ttlist[pos] = (hashtag, total)
                self.ttlist.sort(key=lambda tup: tup[1])
                return
            pos += 1
        if(len(self.ttlist) < config.ttTotalTTs()):
            self.ttlist.append((hashtag,total))
        else:
            if(total > self.ttlist[0][1]): #ttlist should be sorted
                self.ttlist[0] = (hashtag,total)
                self.ttlist.sort(key=lambda tup: tup[1])

    def processBuzz(self,buzz):
        hashtags = buzz.getHashtags()
        for hashtag in hashtags:
            self.loadHashtag(hashtag,str(buzz.uId))

    def processTTRequest(self,request):
        outgoingConnectionManager = ConnectionManager(config.ip(),config.port())
        outgoingConnectionManager.declareQueue(request.requestingUser)
        outgoingConnectionManager.writeToQueue(request.requestingUser, TTResponse(self.ttlist))
        outgoingConnectionManager.close()
        for hashtagTuple in self.ttlist:
            ids = self.getLastMessagesIds(hashtagTuple[0])
            for id in ids:
                self.outgoingBuzzDBConnectionManager.writeToExchange(config.dbExchange(),id,QueryRequest(request.requestingUser,id))

    def getLastMessagesIds(self,hashtag):
        actualDicc = self.hashtagsDicc
        for letter in hashtag:
            actualDicc = actualDicc[letter]
        return actualDicc[config.ttLastMessagesKey()]

    def processRequest(self, ch, method, properties, body):
        messageObject = MessageUtils.deserialize(body)
        if(isinstance(messageObject,Buzz)):
            logging.info("Updating TT")
            self.processBuzz(messageObject)
        elif(isinstance(messageObject,TTRequest)):
            logging.info("Responding with TTs")
            self.processTTRequest(messageObject)
        self.incomingConnectionManager.ack(method.delivery_tag)
        if not self.keepRunning.get():
            self.stop()


    def loadHashtag(self,hashtag,uId):
        hashtag = hashtag[1:] #remove #
        actualDicc = self.hashtagsDicc
        for letter in hashtag:
            if(not actualDicc.has_key(letter)):
                actualDicc[letter] = {config.ttCountKey():0, config.ttLastMessagesKey():collections.deque(maxlen=config.ttMaxMSGS())}
            actualDicc = actualDicc[letter]
        actualDicc[config.ttCountKey()] = actualDicc[config.ttCountKey()] + 1
        if(len(actualDicc[config.ttLastMessagesKey()]) >= config.ttMaxMSGS()):
            actualDicc[config.ttLastMessagesKey()].pop()
        actualDicc[config.ttLastMessagesKey()].appendleft(uId)
        self.updateTrendingTopics(hashtag,actualDicc[config.ttCountKey()])


    def _start(self):
        self.incomingConnectionManager.addTimeout(self.onTimeout)
        self.incomingConnectionManager.listenToQueue(config.ttQueueName(), self.processRequest)
