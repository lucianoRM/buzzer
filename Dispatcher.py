from ActionMessage import ActionMessage, ShutdownSystemPetition, FollowUserPetition, FollowHashtagPetition
from Buzz import Buzz
from ConnectionManager import ConnectionManager
from DBRequest import QueryRequest, DeleteRequest
from MessageUtils import MessageUtils
import logging
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


'''This class handles the redirection of messages. Users can send Buzzes or action messages
to the dispatcher. Then, the dispatcher will redirect or replicate the user messages according to the
how the message should be processed.
If it's a Buzz, it will be sent to the UserRegistraionHandler to nofity registered users and to the 
DBHandler to persist the message.'''
class Dispatcher:

    def __init__(self):
        logging.getLogger(self.__class__.__name__)
        logging.basicConfig(filename="app.log",format='%(levelname)s:%(asctime)s:%(module)s@%(lineno)d:%(message)s', level=logging.INFO)
        self.incomingUserMessagesConnectionManager = ConnectionManager(INCOMING_CONNECTION_IP,INCOMING_CONNECTION_PORT)
        self.incomingUserMessagesConnectionManager.declareQueue(INCOMING_QUEUE_NAME)
        self.incomingUserMessagesConnectionManager.declareQueue(USER_REGISTRATION_QUEUE_NAME)
        self.outgoingUserRegistrationHandlerConnectionManager = ConnectionManager(USER_REGISTRATION_HANDLER_IP,USER_REGISTRATION_HANLDER_PORT)
        self.outgoingIndexConnectionManager = ConnectionManager(INDEX_HANDLER_IP,INDEX_HANDLER_PORT)
        self.outgoingIndexConnectionManager.declareExchange(INDEX_HANDLER_EXCHANGE_NAME)
        self.outgoingBuzzDBConnectionManager = ConnectionManager(BUZZ_DB_HANDLER_IP,BUZZ_DB_HANDLER_PORT)
        self.outgoingBuzzDBConnectionManager.declareExchange(BUZZ_DB_HANDLER_EXCHANGE_NAME)


    def handleBuzz(self,buzz):
        logging.info("Processing buzz")
        self.outgoingUserRegistrationHandlerConnectionManager.writeToQueue(USER_REGISTRATION_QUEUE_NAME,buzz)
        self.outgoingBuzzDBConnectionManager.writeToExchange(BUZZ_DB_HANDLER_EXCHANGE_NAME,str(buzz.uId),buzz)
        self.outgoingIndexConnectionManager.writeToExchange(INDEX_HANDLER_EXCHANGE_NAME,buzz.user,buzz)

    def handleFollowingPetition(self,petition):
        logging.info("Processing following petition")
        self.outgoingUserRegistrationHandlerConnectionManager.writeToQueue(USER_REGISTRATION_QUEUE_NAME, petition)

    def handleQueryRequest(self, request):
        logging.info("Processing query request ")
        self.outgoingIndexConnectionManager.writeToExchange(INDEX_HANDLER_EXCHANGE_NAME,request.tag,request)

    def handleDeleteRequest(self, request):
        logging.info("Processing delete request")
        self.outgoingBuzzDBConnectionManager.writeToExchange(BUZZ_DB_HANDLER_EXCHANGE_NAME,str(request.uId),request)

    def handleShutdown(self,shutdownmessage):
        '''When shutdown, the message should be sent to every node'''
        self.outgoingUserRegistrationHandlerConnectionManager.writeToQueue(USER_REGISTRATION_QUEUE_NAME,shutdownmessage)
        self.outgoingUserRegistrationHandlerConnectionManager.stopListeningToQueue()
        self.incomingUserMessagesConnectionManager.close()
        self.outgoingUserRegistrationHandlerConnectionManager.close()

    def onMessageReceived(self, channel, method, properties, body):
        logging.info("Received message")
        message = MessageUtils.deserialize(body)
        if(isinstance(message,Buzz)):
            self.handleBuzz(message)
        elif(isinstance(message,FollowUserPetition)):
            self.handleFollowingPetition(message)
        elif(isinstance(message,FollowHashtagPetition)):
            self.handleFollowingPetition(message)
        elif(isinstance(message,QueryRequest)):
            self.handleQueryRequest(message)
        elif(isinstance(message,DeleteRequest)):
            self.handleDeleteRequest(message)
        elif(isinstance(message,ShutdownSystemPetition)):
            self.incomingUserMessagesConnectionManager.ack(method.delivery_tag)
            self.handleShutdown(message)
            return
        self.incomingUserMessagesConnectionManager.ack(method.delivery_tag)

    def start(self):
        self.incomingUserMessagesConnectionManager.listenToQueue(INCOMING_QUEUE_NAME,self.onMessageReceived)
