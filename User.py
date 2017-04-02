from ConnectionManager import ConnectionManager
from Notifier import Notifier
import pickle

from QueueManager import QueueManager

QUEUE_IP = 'localhost'
QUEUE_PORT = 5672
SERVER_QUEUE_NAME = 'buzzer'

'''
Handles the client buzzer. It's in charge or publishing new buzzes and can notify when
a new registered channel is updated
'''
class User:

    connectionManager = None
    queueManager = None


    def __init__(self, name):
        self.name = name
        self.notifier = Notifier(self.name)
        self.connectionManager = ConnectionManager(QUEUE_IP,QUEUE_PORT)
        self.queueManager = QueueManager(self.connectionManager, SERVER_QUEUE_NAME)

    def startNotificationThread(self):
        self.notifier.startListeningForNotifications()

    def stopNotificationThread(self):
        self.notifier.stopListeningForNotifications()

    def turnNotificationsOn(self):
        self.startNotificationThread()

    def turnNotificationsOff(self):
        self.stopNotificationThread()

    def send(self, message):
        self.queueManager.writeToQueue(message)












