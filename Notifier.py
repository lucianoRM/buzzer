import uuid
from threading import Thread
import pickle
from Buzz import Buzz
from ConnectionManager import ConnectionManager
from QueueManager import QueueManager


'''Creates a new channel for receiving user notifications'''
class Notifier:

    listeningThread = None
    EXIT_KEY = str(uuid.uuid4())

    def __init__(self,queueName):
        self.connectionManager = ConnectionManager()
        self.queueManager = QueueManager(self.connectionManager, queueName)
        self.queueName = queueName

    def notificationCallback(self, channel, method, properties, body):
        if(body == self.EXIT_KEY):
            self.connectionManager.channel.basic_ack(method.delivery_tag)
            self.queueManager.stopListeningToQueue()
        else:
            buzz = pickle.loads(body)
            if(isinstance(buzz,Buzz)):
                print buzz.message
                print buzz.user
                print buzz.uId
                self.connectionManager.channel.basic_ack(method.delivery_tag)


    def _startListeningForNotifications(self):
        self.queueManager.listenToQueue(self.notificationCallback)

    def startListeningForNotifications(self):
        self.listeningThread = Thread(None, self._startListeningForNotifications)
        self.listeningThread.start()

    def stopListeningForNotifications(self):
        connectionManager = ConnectionManager()
        queueManager = QueueManager(connectionManager, self.queueName)
        queueManager.writeToQueue(self.EXIT_KEY)
        self.listeningThread.join()


