from ActionMessage import ActionMessage, ShutdownSystemPetition, FollowUserPetition, FollowHashtagPetition
from Buzz import Buzz
from ConnectionManager import ConnectionManager
from MessageUtils import MessageUtils

INCOMING_CONNECTION_IP = 'localhost'
INCOMING_CONNECTION_PORT = 5672
INCOMING_QUEUE_NAME = 'buzzer_main'
USER_REGISTRATION_HANDLER_IP = 'localhost'
USER_REGISTRATION_HANLDER_PORT = 5672
USER_REGISTRATION_QUEUE_NAME = 'dispatcher-registrationhandler'

'''This class handles the redirection of messages. Users can send Buzzes or action messages
to the dispatcher. Then, the dispatcher will redirect or replicate the user messages according to the
how the message should be processed.
If it's a Buzz, it will be sent to the UserRegistraionHandler to nofity registered users and to the 
DBHandler to persist the message.'''
class Dispatcher:

    def __init__(self):
        self.incomingUserMessagesConnectionManager = ConnectionManager(INCOMING_CONNECTION_IP,INCOMING_CONNECTION_PORT)
        self.incomingUserMessagesConnectionManager.declareQueue(INCOMING_QUEUE_NAME)
        self.outgoingUserRegistrationHandlerConnectionManager = ConnectionManager(USER_REGISTRATION_HANDLER_IP,USER_REGISTRATION_HANLDER_PORT)
        self.incomingUserMessagesConnectionManager.declareQueue(USER_REGISTRATION_QUEUE_NAME)

    def handleBuzz(self,buzz):
        print "It's a buzz"
        self.outgoingUserRegistrationHandlerConnectionManager.writeToQueue(USER_REGISTRATION_QUEUE_NAME,buzz)

    def handleFollowingPetition(self,petition):
        print "It's a petition!"
        self.outgoingUserRegistrationHandlerConnectionManager.writeToQueue(USER_REGISTRATION_QUEUE_NAME, petition)

    def handleShutdown(self,shutdownmessage):
        '''When shutdown, the message should be sent to every node'''
        self.outgoingUserRegistrationHandlerConnectionManager.writeToQueue(USER_REGISTRATION_QUEUE_NAME,shutdownmessage)
        self.outgoingUserRegistrationHandlerConnectionManager.stopListeningToQueue()
        self.incomingUserMessagesConnectionManager.close()
        self.outgoingUserRegistrationHandlerConnectionManager.close()

    def onMessageReceived(self, channel, method, properties, body):
        message = MessageUtils.deserialize(body)
        if(isinstance(message,Buzz)):
            self.handleBuzz(message)
        elif(isinstance(message,FollowUserPetition)):
            self.handleFollowingPetition(message)
        elif(isinstance(message,FollowHashtagPetition)):
            self.handleFollowingPetition(message)
        elif(isinstance(message,ShutdownSystemPetition)):
            self.incomingUserMessagesConnectionManager.channel.basic_ack(method.delivery_tag)
            self.handleShutdown(message)
            return
        self.incomingUserMessagesConnectionManager.channel.basic_ack(method.delivery_tag)

    def start(self):
        self.incomingUserMessagesConnectionManager.listenToQueue(INCOMING_QUEUE_NAME,self.onMessageReceived)
        "Exited!"

dispatcher = Dispatcher()
dispatcher.start()