import pickle

import ActionMessage
import Buzz
from ConnectionManager import ConnectionManager
from QueueManager import QueueManager

INCOMING_QUEUE_IP = 'localhost'
INCOMING_QUEUE_PORT = 5672
OUTGOING_QUEUE_IP = 'localhost'
OUTGOING_QUEUE_PORT = 5672
INCOMING_QUEUE_NAME = 'buzzer'

class UserRegistrationHandler:



    def __init__(self):
        self.incomingConnectionManager = ConnectionManager(INCOMING_QUEUE_IP,INCOMING_QUEUE_PORT)
        self.outgoingConnectionManager = ConnectionManager(OUTGOING_QUEUE_IP,OUTGOING_QUEUE_PORT)
        self.incomingQueueManager = QueueManager(self.incomingConnectionManager, INCOMING_QUEUE_NAME)

    def onMessageReceived(self, channel, method, properties, body):
        try:
            message = pickle.loads(body)
        except:
            print "Error deserializing"
        if(isinstance(message, ActionMessage.FollowUserPetition)):
            print "Is follow user"
        elif(isinstance(message, Buzz.Buzz)):
            print "Is Buzz"
        elif(isinstance(message, ActionMessage.ShutdownSystemPetition)):
            self.incomingConnectionManager.channel.basic_ack(method.delivery_tag)
            self.terminate()
            return

        self.incomingConnectionManager.channel.basic_ack(method.delivery_tag)

    def waitForMessages(self):
        self.incomingQueueManager.listenToQueue(self.onMessageReceived)

    def terminate(self):
        self.incomingQueueManager.stopListeningToQueue()
        self.outgoingConnectionManager.close()
        self.incomingConnectionManager.close()
