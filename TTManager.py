import logging

from Buzz import Buzz
from ConnectionManager import ConnectionManager
from GenericListener import GenericListener
from MessageUtils import MessageUtils
from TrendingTopic import TTRequest, TTResponse

INCOMING_CONNECTION_IP = "localhost"
INCOMING_CONNECTION_PORT = 5672
OUTGOING_CONNECTION_IP = "localhost"
OUTGOING_CONNECTION_PORT = 5672
QUEUE_NAME = 'tt-queue'
COUNT_KEY = "total"
TOTAL_TTS = 3

class TTManager(GenericListener):

    def __init__(self):
        GenericListener.__init__(self,INCOMING_CONNECTION_IP,INCOMING_CONNECTION_PORT)
        self.incomingConnectionManager.declareQueue(QUEUE_NAME)
        self.hashtagsDicc = {}
        self.ttlist = []

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
            self.loadHashtag(hashtag)

    def processTTRequest(self,request):
        outgoingConnectionManager = ConnectionManager(OUTGOING_CONNECTION_IP, OUTGOING_CONNECTION_PORT)
        outgoingConnectionManager.declareQueue(request.requestingUser)
        outgoingConnectionManager.writeToQueue(request.requestingUser, TTResponse(self.ttlist))
        outgoingConnectionManager.close()

    def processRequest(self, ch, method, properties, body):
        messageObject = MessageUtils.deserialize(body)
        if(isinstance(messageObject,Buzz)):
            logging.info("Updating TT")
            self.processBuzz(messageObject)
        elif(isinstance(messageObject,TTRequest)):
            logging.info("Responding with TTs")
            self.processTTRequest(messageObject)
        self.incomingConnectionManager.ack(method.delivery_tag)


    def loadHashtag(self,hashtag):
        hashtag = hashtag[1:] #remove #
        actualDicc = self.hashtagsDicc
        for letter in hashtag:
            if(not actualDicc.has_key(letter)):
                actualDicc[letter] = {COUNT_KEY:0}
            actualDicc = actualDicc[letter]
        actualDicc[COUNT_KEY] = actualDicc[COUNT_KEY] + 1
        self.updateTrendingTopics(hashtag,actualDicc[COUNT_KEY])


    def _start(self):
        self.incomingConnectionManager.addTimeout(self.onTimeout)
        self.incomingConnectionManager.listenToQueue(QUEUE_NAME, self.processRequest)
