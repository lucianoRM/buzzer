import threading

import time

import signal

from ActionMessage import FollowHashtagPetition, FollowUserPetition, ShutdownSystemPetition
from Buzz import Buzz
from ConnectionManager import ConnectionManager
from DBRequest import QueryRequest, DeleteRequest
from Notifier import Notifier
import logging
logging.getLogger("pika").setLevel(logging.WARNING)



QUEUE_IP = 'localhost'
QUEUE_PORT = 5672
SERVER_QUEUE_NAME = 'buzzer_main'

'''
Handles the client buzzer. It's in charge or publishing new buzzes and can notify when
a new registered channel is updated
'''
class User:

    connectionManager = None


    def __init__(self, name):
        logging.getLogger(self.__class__.__name__)
        logging.basicConfig(filename="app.log",format='%(levelname)s:%(asctime)s:%(module)s@%(lineno)d:%(message)s', level=logging.INFO)
        self.name = name
        self.notifier = Notifier(self.name)
        self.connectionManager = ConnectionManager(QUEUE_IP,QUEUE_PORT)
        self.connectionManager.declareQueue(SERVER_QUEUE_NAME)
        #signal.signal(signal.SIGINT, self.turnNotificationsOff)


    def turnNotificationsOn(self,conditionVariable):
        self.notifier.start(conditionVariable)

    def turnNotificationsOff(self):
        self.notifier.stop()

    def sendBuzz(self, message):
        buzz = Buzz(self.name, message)
        self.connectionManager.writeToQueue(SERVER_QUEUE_NAME,buzz)
        logging.info("Sent buzz")
        return str(buzz.uId)

    def sendFollowHashtagPetition(self, hashtag):
        petition = FollowHashtagPetition(self.name, hashtag)
        self.connectionManager.writeToQueue(SERVER_QUEUE_NAME,petition)
        logging.info("Sent follow hashtag petition")

    def sendFollowUserPetition(self, otherUser):
        petition = FollowUserPetition(self.name, otherUser)
        self.connectionManager.writeToQueue(SERVER_QUEUE_NAME,petition)
        logging.info("Sent follow user petition")

    def sendRequestMessage(self, tag):
        request = QueryRequest(self.name,tag)
        self.connectionManager.writeToQueue(SERVER_QUEUE_NAME,request)

    def sendDeleteMessage(self, uId):
        request = DeleteRequest(self.name,uId)
        self.connectionManager.writeToQueue(SERVER_QUEUE_NAME,request)
        logging.info("Sent delete message")














