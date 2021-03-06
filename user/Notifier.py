import uuid

from exception.InvalidMessageException import InvalidMessageException
from messages.Buzz import Buzz
from messages.TrendingTopic import TTResponse
from listener.GenericListener import GenericListener
from utils.MessageUtils import MessageUtils

QUEUE_IP = 'localhost'
QUEUE_PORT = 5672

'''Creates a new channel for receiving user notifications'''
class Notifier(GenericListener):

    listeningThread = None
    EXIT_KEY = str(uuid.uuid4())

    def __init__(self,queueName):
        GenericListener.__init__(self,QUEUE_IP,QUEUE_PORT)
        self.queueName = queueName
        self.incomingConnectionManager.declareQueue(queueName)


    def notificationCallback(self, channel ,method ,properties ,body):
        try:
            obj = MessageUtils.deserialize(body)
        except InvalidMessageException as e:
            self.incomingConnectionManager.ack(method.delivery_tag)
            raise e
        if(isinstance(obj,TTResponse)):
            print "\n=====TrendingTopics======"
            for tt in obj.valuesList:
                print tt[0] + " : " + str(tt[1])
        elif(isinstance(obj,Buzz)):
            print "\n======" + self.queueName + ":Buzz received======="
            print "ID:" + str(obj.uId)
            print "USER: " + obj.user
            print "MSG: " + obj.message
            print "\n"
        self.incomingConnectionManager.ack(method.delivery_tag)
        if not self.keepRunning.get():
            self.stop()

    def _start(self):
        self.incomingConnectionManager.addTimeout(self.onTimeout)
        self.incomingConnectionManager.listenToQueue(self.queueName,self.notificationCallback)









