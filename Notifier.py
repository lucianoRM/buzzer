import uuid
from threading import Thread
from Buzz import Buzz
from ConnectionManager import ConnectionManager
from InvalidMessageException import InvalidMessageException
from MessageUtils import MessageUtils

QUEUE_IP = 'localhost'
QUEUE_PORT = 5672

'''Creates a new channel for receiving user notifications'''
class Notifier:

    listeningThread = None
    EXIT_KEY = str(uuid.uuid4())

    def __init__(self,queueName):
        self.connectionManager = ConnectionManager(QUEUE_IP,QUEUE_PORT)
        self.queueName = queueName
        self.connectionManager.declareQueue(queueName)

    def notificationCallback(self, channel, method, properties, body):
        if(body == self.EXIT_KEY):
            self.connectionManager.stopListeningToQueue()
        else:
            try:
                buzz = MessageUtils.deserialize(body)
            except InvalidMessageException as e:
                self.connectionManager.ack(method.delivery_tag)
                raise e
            print buzz.message
            print buzz.user
            print buzz.uId
            self.connectionManager.ack(method.delivery_tag)




    def _startListeningForNotifications(self):
        self.connectionManager.listenToQueue(self.queueName,self.notificationCallback)

    def startListeningForNotifications(self):
        self.listeningThread = Thread(None, self._startListeningForNotifications)
        self.listeningThread.start()

    def stopListeningForNotifications(self):
        connectionManager = ConnectionManager(QUEUE_IP,QUEUE_PORT)
        connectionManager.declareQueue(self.queueName)
        connectionManager.writeToQueue(self.queueName,self.EXIT_KEY)
        self.listeningThread.join()


