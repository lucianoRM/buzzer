import uuid
from threading import Thread

import time

from Buzz import Buzz
from ConnectionManager import ConnectionManager
from GenericListener import GenericListener
from InvalidMessageException import InvalidMessageException
from MessageUtils import MessageUtils
import signal

from ThreadSafeVariable import ThreadSafeVariable

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
        if(body == self.EXIT_KEY):
            self.incomingConnectionManager.stopListeningToQueue()
        else:
            try:
                buzz = MessageUtils.deserialize(body)
            except InvalidMessageException as e:
                self.incomingConnectionManager.ack(method.delivery_tag)
                raise e
            print "\n======" + self.queueName + ":Buzz received======="
            print "ID" + str(buzz.uId)
            print "USER: " + buzz.user
            print "MSG: " + buzz.message
            print "\n"
            self.incomingConnectionManager.ack(method.delivery_tag)

    def _start(self):
        self.incomingConnectionManager.addTimeout(self.onTimeout)
        self.incomingConnectionManager.listenToQueue(self.queueName,self.notificationCallback)









