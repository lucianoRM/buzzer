from Notifier import Notifier
import pickle

'''
Handles the client buzzer. It's in charge or publishing new buzzes and can notify when
a new registered channel is updated
'''
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











