import logging

from GenericListener import GenericListener
from config import config
from connection.ConnectionManager import ConnectionManager
from db.DBRequest import QueryRequest, DeleteRequest
from messages.ActionMessage import FollowUserPetition, FollowHashtagPetition
from messages.Buzz import Buzz
from messages.TrendingTopic import TTRequest
from utils.MessageUtils import MessageUtils

logging.getLogger("pika").setLevel(logging.WARNING)




'''This class handles the redirection of messages. Users can send Buzzes or action messages
to the dispatcher. Then, the dispatcher will redirect or replicate the user messages according to the
how the message should be processed.
If it's a Buzz, it will be sent to the UserRegistraionHandler to nofity registered users and to the 
DBHandler to persist the message.'''
class Dispatcher(GenericListener):

    def __init__(self):
        GenericListener.__init__(self,config.ip(),config.port())
        self.logger = logging.getLogger("dispatcher")
        formatter = logging.Formatter('%(levelname)s:%(asctime)s:%(module)s@%(lineno)d:%(message)s')
        fileHandler = logging.FileHandler("dispatcher.log", mode='w')
        fileHandler.setFormatter(formatter)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(fileHandler)

        self.incomingConnectionManager.declareQueue(config.dispatcherQueue())
        self.outgoingUserRegistrationHandlerConnectionManager = ConnectionManager(config.ip(),config.port())
        self.outgoingUserRegistrationHandlerConnectionManager.declareQueue(config.registrationQueueName())
        self.outgoingIndexConnectionManager = ConnectionManager(config.ip(),config.port())
        self.outgoingIndexConnectionManager.declareExchange(config.indexExchange())
        self.outgoingBuzzDBConnectionManager = ConnectionManager(config.ip(),config.port())
        self.outgoingBuzzDBConnectionManager.declareExchange(config.dbExchange())
        self.outgoingTTConnectionManager = ConnectionManager(config.ip(),config.port())
        self.outgoingTTConnectionManager.declareQueue(config.ttQueueName())


    def handleBuzz(self,buzz):
        logging.info("Processing buzz")
        self.outgoingUserRegistrationHandlerConnectionManager.writeToQueue(config.registrationQueueName(),buzz)
        self.outgoingBuzzDBConnectionManager.writeToExchange(config.dbExchange(),str(buzz.uId),buzz)
        self.outgoingIndexConnectionManager.writeToExchange(config.indexExchange(),buzz.user,buzz)
        self.outgoingTTConnectionManager.writeToQueue(config.ttQueueName(),buzz)

    def handleFollowingPetition(self,petition):
        logging.info("Processing following petition")
        self.outgoingUserRegistrationHandlerConnectionManager.writeToQueue(config.registrationQueueName(), petition)

    def handleQueryRequest(self, request):
        logging.info("Processing query request ")
        self.outgoingIndexConnectionManager.writeToExchange(config.indexExchange(),request.tag,request)

    def handleDeleteRequest(self, request):
        logging.info("Processing delete request")
        self.outgoingBuzzDBConnectionManager.writeToExchange(config.dbExchange(),str(request.uId),request)

    def handleTTRequest(self,request):
        logging.info("Processing tt request")
        self.outgoingTTConnectionManager.writeToQueue(config.ttQueueName(),request)

    def onMessageReceived(self, channel, method, properties, body):
        logging.info("Received message")
        self.logger.info("Received message")
        message = MessageUtils.deserialize(body)
        if(isinstance(message,Buzz)):
            self.logger.info("It's a buzz: [" + str(message.uId) + ";" + message.user + ";" + message.message + ']')
            self.handleBuzz(message)
        elif(isinstance(message,FollowUserPetition)):
            self.logger.info("It's a following petition from: " + message.user + " to follow: " + message.otherUser)
            self.handleFollowingPetition(message)
        elif(isinstance(message,FollowHashtagPetition)):
            self.logger.info("It's a following petition from: " + message.user + " to follow: " + message.hashtag)
            self.handleFollowingPetition(message)
        elif(isinstance(message,QueryRequest)):
            self.logger.info("It's a request for buzzes from: " + message.user + " to get: " + message.tag)
            self.handleQueryRequest(message)
        elif(isinstance(message,DeleteRequest)):
            self.logger.info("It's a delete request from: " + message.user + " to delete: " + str(message.uId))
            self.handleDeleteRequest(message)
        elif(isinstance(message,TTRequest)):
            self.logger.info("It's a request for trending topics from: " + message.requestingUser)
            self.handleTTRequest(message)
        self.incomingConnectionManager.ack(method.delivery_tag)
        if not self.keepRunning.get():
            self.stop()


    def _start(self):
         self.incomingConnectionManager.addTimeout(self.onTimeout)
         self.incomingConnectionManager.listenToQueue(config.dispatcherQueue(), self.onMessageReceived)


