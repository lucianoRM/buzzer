
from ActionMessage import ActionMessage, ShutdownSystemPetition, FollowUserPetition, FollowHashtagPetition
from Buzz import Buzz
from ConnectionManager import ConnectionManager
from DBRequest import QueryRequest, DeleteRequest
from GenericListener import GenericListener
from MessageUtils import MessageUtils
import logging

from TrendingTopic import TTRequest

logging.getLogger("pika").setLevel(logging.WARNING)



INCOMING_CONNECTION_IP = 'localhost'
INCOMING_CONNECTION_PORT = 5672
INCOMING_QUEUE_NAME = 'buzzer_main'
USER_REGISTRATION_HANDLER_IP = 'localhost'
USER_REGISTRATION_HANLDER_PORT = 5672
USER_REGISTRATION_QUEUE_NAME = 'dispatcher-registrationhandler'
INDEX_HANDLER_IP = 'localhost'
INDEX_HANDLER_PORT = 5672
INDEX_HANDLER_EXCHANGE_NAME = 'index-exchange'
BUZZ_DB_HANDLER_IP = 'localhost'
BUZZ_DB_HANDLER_PORT = 5672
BUZZ_DB_HANDLER_EXCHANGE_NAME = 'buzz-exchange'
TT_HANDLER_IP = 'localhost'
TT_HANDLER_PORT = 5672
TT_HANDLER_QUEUE_NAME = 'tt-queue'


'''This class handles the redirection of messages. Users can send Buzzes or action messages
to the dispatcher. Then, the dispatcher will redirect or replicate the user messages according to the
how the message should be processed.
If it's a Buzz, it will be sent to the UserRegistraionHandler to nofity registered users and to the 
DBHandler to persist the message.'''
class Dispatcher(GenericListener):

    def __init__(self):
        GenericListener.__init__(self,INCOMING_CONNECTION_IP,INCOMING_CONNECTION_PORT)
        self.logger = logging.getLogger("dispatcher")
        formatter = logging.Formatter('%(levelname)s:%(asctime)s:%(module)s@%(lineno)d:%(message)s')
        fileHandler = logging.FileHandler("dispatcher.log", mode='w')
        fileHandler.setFormatter(formatter)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(fileHandler)
        self.logger.addHandler(fileHandler)

        self.incomingConnectionManager.declareQueue(INCOMING_QUEUE_NAME)
        self.incomingConnectionManager.declareQueue(USER_REGISTRATION_QUEUE_NAME)
        self.outgoingUserRegistrationHandlerConnectionManager = ConnectionManager(USER_REGISTRATION_HANDLER_IP,USER_REGISTRATION_HANLDER_PORT)
        self.outgoingUserRegistrationHandlerConnectionManager.declareQueue(USER_REGISTRATION_QUEUE_NAME)
        self.outgoingIndexConnectionManager = ConnectionManager(INDEX_HANDLER_IP,INDEX_HANDLER_PORT)
        self.outgoingIndexConnectionManager.declareExchange(INDEX_HANDLER_EXCHANGE_NAME)
        self.outgoingBuzzDBConnectionManager = ConnectionManager(BUZZ_DB_HANDLER_IP,BUZZ_DB_HANDLER_PORT)
        self.outgoingBuzzDBConnectionManager.declareExchange(BUZZ_DB_HANDLER_EXCHANGE_NAME)
        self.outgoingTTConnectionManager = ConnectionManager(TT_HANDLER_IP,TT_HANDLER_PORT)
        self.outgoingTTConnectionManager.declareQueue(TT_HANDLER_QUEUE_NAME)


    def handleBuzz(self,buzz):
        logging.info("Processing buzz")
        self.outgoingUserRegistrationHandlerConnectionManager.writeToQueue(USER_REGISTRATION_QUEUE_NAME,buzz)
        self.outgoingBuzzDBConnectionManager.writeToExchange(BUZZ_DB_HANDLER_EXCHANGE_NAME,".".join(list(str(buzz.uId))),buzz)
        self.outgoingIndexConnectionManager.writeToExchange(INDEX_HANDLER_EXCHANGE_NAME,".".join(list(buzz.user)),buzz)
        self.outgoingTTConnectionManager.writeToQueue(TT_HANDLER_QUEUE_NAME,buzz)

    def handleFollowingPetition(self,petition):
        logging.info("Processing following petition")
        self.outgoingUserRegistrationHandlerConnectionManager.writeToQueue(USER_REGISTRATION_QUEUE_NAME, petition)

    def handleQueryRequest(self, request):
        logging.info("Processing query request ")
        self.outgoingIndexConnectionManager.writeToExchange(INDEX_HANDLER_EXCHANGE_NAME,request.tag,request)

    def handleDeleteRequest(self, request):
        logging.info("Processing delete request")
        self.outgoingBuzzDBConnectionManager.writeToExchange(BUZZ_DB_HANDLER_EXCHANGE_NAME,str(request.uId),request)

    def handleTTRequest(self,request):
        logging.info("Processing tt request")
        self.outgoingTTConnectionManager.writeToQueue(TT_HANDLER_QUEUE_NAME,request)

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


    def _start(self):
         self.incomingConnectionManager.addTimeout(self.onTimeout)
         self.incomingConnectionManager.listenToQueue(INCOMING_QUEUE_NAME, self.onMessageReceived)


