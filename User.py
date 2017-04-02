from ActionMessage import FollowHashtagPetition, FollowUserPetition, ShutdownSystemPetition
from Buzz import Buzz
from ConnectionManager import ConnectionManager
from Notifier import Notifier


QUEUE_IP = 'localhost'
QUEUE_PORT = 5672
SERVER_QUEUE_NAME = 'buzzer_main'

'''
Handles the client buzzer. It's in charge or publishing new buzzes and can notify when
a new registered channel is updated
'''
class User:

    connectionManager = None


    def __init__(self, name):
        self.name = name
        self.notifier = Notifier(self.name)
        self.connectionManager = ConnectionManager(QUEUE_IP,QUEUE_PORT)
        self.connectionManager.declareQueue(SERVER_QUEUE_NAME)

    def startNotificationThread(self):
        self.notifier.startListeningForNotifications()

    def stopNotificationThread(self):
        self.notifier.stopListeningForNotifications()

    def turnNotificationsOn(self):
        self.startNotificationThread()

    def turnNotificationsOff(self):
        self.stopNotificationThread()

    def sendBuzz(self, message):
        buzz = Buzz(self.name, message)
        self.connectionManager.writeToQueue(SERVER_QUEUE_NAME,buzz)

    def sendFollowHashtagPetition(self, hashtag):
        petition = FollowHashtagPetition(self.name, hashtag)
        self.connectionManager.writeToQueue(SERVER_QUEUE_NAME,petition)

    def sendFollowUserPetition(self, otherUser):
        petition = FollowUserPetition(self.name, otherUser)
        self.connectionManager.writeToQueue(SERVER_QUEUE_NAME,petition)

    def sendShutdownPetition(self):
        petition = ShutdownSystemPetition(self.name)
        self.connectionManager.writeToQueue(SERVER_QUEUE_NAME,petition)













