import uuid
from threading import Thread
from Buzz import Buzz
from ConnectionManager import ConnectionManager
from Finisher import Finisher
from InvalidMessageException import InvalidMessageException
from MessageUtils import MessageUtils
import signal

from ThreadSafeVariable import ThreadSafeVariable

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
        self.keepRunning = ThreadSafeVariable(True)
        signal.signal(signal.SIGINT, self.stopListeningForNotifications)

    def timeoutCallback(self):
        if(not self.keepRunning.get()):
            self.connectionManager.stopListeningToQueue()
            return
        self.connectionManager.addTimeout(self.timeoutCallback)

    def notificationCallback(self, channel ,method ,properties ,body):
        if(body == self.EXIT_KEY):
            self.connectionManager.stopListeningToQueue()
        else:
            try:
                buzz = MessageUtils.deserialize(body)
            except InvalidMessageException as e:
                self.connectionManager.ack(method.delivery_tag)
                raise e
            print "\n======" + self.queueName + ":Buzz received======="
            print "ID" + str(buzz.uId)
            print "USER: " + buzz.user
            print "MSG: " + buzz.message
            print "\n"
            self.connectionManager.ack(method.delivery_tag)




    def _startListeningForNotifications(self):
        self.connectionManager.addTimeout(self.timeoutCallback)
        self.connectionManager.listenToQueue(self.queueName,self.notificationCallback)

    def startListeningForNotifications(self):
        self.listeningThread = Thread(target=self._startListeningForNotifications)
        self.listeningThread.start()
        signal.pause()

    def stopListeningForNotifications(self,signal,frame):
        self.keepRunning.set(False)
        self.listeningThread.join()
        self.connectionManager.close()
        


a = Notifier("luciano")
a.startListeningForNotifications()
