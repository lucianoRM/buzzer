import collections
import logging

from connection.ConnectionManager import ConnectionManager
from db.DBRequest import QueryRequest
from messages.Buzz import Buzz
from messages.TrendingTopic import TTRequest, TTResponse
from listener.GenericListener import GenericListener
from utils.MessageUtils import MessageUtils

INCOMING_CONNECTION_IP = "localhost"
INCOMING_CONNECTION_PORT = 5672
OUTGOING_CONNECTION_IP = "localhost"
OUTGOING_CONNECTION_PORT = 5672
QUEUE_NAME = 'tt-queue'
BUZZ_DB_HANDLER_IP = 'localhost'
BUZZ_DB_HANDLER_PORT = 5672
BUZZ_DB_HANDLER_EXCHANGE_NAME = 'buzz-exchange'
COUNT_KEY = "total"
LAST_MESSAGES_KEY = "last_messages"
TOTAL_TTS = 3
MAX_MSGS = 3

class TTManager(GenericListener):

    def __init__(self):
        GenericListener.__init__(self,INCOMING_CONNECTION_IP,INCOMING_CONNECTION_PORT)
        self.incomingConnectionManager.declareQueue(QUEUE_NAME)
        self.hashtagsDicc = {}
        self.ttlist = []
        self.outgoingBuzzDBConnectionManager = ConnectionManager(BUZZ_DB_HANDLER_IP, BUZZ_DB_HANDLER_PORT)
        self.outgoingBuzzDBConnectionManager.declareExchange(BUZZ_DB_HANDLER_EXCHANGE_NAME)

    def updateTrendingTopics(self,hashtag,total):
        pos = 0
        for tuple in self.ttlist:
            if (tuple[0] == hashtag):
                self.ttlist[pos] = (hashtag, total)
                self.ttlist.sort(key=lambda tup: tup[1])
                return
            pos += 1
        if(len(self.ttlist) < TOTAL_TTS):
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
        outgoingConnectionManager = ConnectionManager(OUTGOING_CONNECTION_IP, OUTGOING_CONNECTION_PORT)
        outgoingConnectionManager.declareQueue(request.requestingUser)
        outgoingConnectionManager.writeToQueue(request.requestingUser, TTResponse(self.ttlist))
        outgoingConnectionManager.close()
        for hashtagTuple in self.ttlist:
            ids = self.getLastMessagesIds(hashtagTuple[0])
            for id in ids:
                self.outgoingBuzzDBConnectionManager.writeToExchange(BUZZ_DB_HANDLER_EXCHANGE_NAME,id,QueryRequest(request.requestingUser,id))

    def getLastMessagesIds(self,hashtag):
        actualDicc = self.hashtagsDicc
        for letter in hashtag:
            actualDicc = actualDicc[letter]
        return actualDicc[LAST_MESSAGES_KEY]

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
                actualDicc[letter] = {COUNT_KEY:0, LAST_MESSAGES_KEY:collections.deque(maxlen=MAX_MSGS)}
            actualDicc = actualDicc[letter]
        actualDicc[COUNT_KEY] = actualDicc[COUNT_KEY] + 1
        if(len(actualDicc[LAST_MESSAGES_KEY]) >= MAX_MSGS):
            actualDicc[LAST_MESSAGES_KEY].pop()
        actualDicc[LAST_MESSAGES_KEY].appendleft(uId)
        self.updateTrendingTopics(hashtag,actualDicc[COUNT_KEY])


    def _start(self):
        self.incomingConnectionManager.addTimeout(self.onTimeout)
        self.incomingConnectionManager.listenToQueue(QUEUE_NAME, self.processRequest)
