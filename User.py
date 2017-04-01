from ConnectionManager import ConnectionManager
from Notifier import Notifier
from QueueManager import QueueManager
from threading import Thread


class User:

    connectionManager = None
    queueManager = None


    def __init__(self, name):
        self.name = name
        self.listeningThread = None
        self.notifier = Notifier(self.name)


    def startNotificationThread(self):
        self.notifier.startListeningForNotifications()

    def stopNotificationThread(self):
        self.notifier.stopListeningForNotifications()

    def login(self):
        self.startNotificationThread()

    def logout(self):
        self.stopNotificationThread()











