import uuid
from threading import Thread
from Buzz import Buzz
from ConnectionManager import ConnectionManager
from QueueManager import QueueManager

QUEUE_IP = 'localhost'
QUEUE_PORT = 5672

'''Creates a new channel for receiving user notifications'''
class Notifier:

    listeningThread = None
    EXIT_KEY = str(uuid.uuid4())

    def __init__(self,queueName):
        self.connectionManager = ConnectionManager(QUEUE_IP,QUEUE_PORT)
        self.queueManager = QueueManager(self.connectionManager, queueName)
        self.queueName = queueName

    def notificationCallback(self, channel, method, properties, body):
        if(body == self.EXIT_KEY):
            self.queueManager.stopListeningToQueue()
        else:
            try:
                buzz = Buzz(body)
                print buzz.message
                print buzz.user
                print buzz.uId
            except:
                print "Not a Buzz"
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


