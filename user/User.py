import logging

from Notifier import Notifier
from config import config
from connection.ConnectionManager import ConnectionManager
from db.DBRequest import QueryRequest, DeleteRequest
from messages.ActionMessage import FollowHashtagPetition, FollowUserPetition
from messages.Buzz import Buzz
from messages.TrendingTopic import TTRequest

logging.getLogger("pika").setLevel(logging.WARNING)



'''
Handles the client buzzer. It's in charge or publishing new buzzes and can notify when
a new registered channel is updated
'''
class User:

    connectionManager = None


    def __init__(self, name):
        logging.getLogger(self.__class__.__name__)
        logging.basicConfig(filename="app.log",format='%(levelname)s:%(asctime)s:%(module)s@%(lineno)d:%(message)s', level=logging.INFO)
        self.name = name
        self.notifier = Notifier(self.name)
        self.connectionManager = ConnectionManager(config.ip(),config.port())
        self.connectionManager.declareQueue(config.dispatcherQueue())
        #signal.signal(signal.SIGINT, self.turnNotificationsOff)


    def turnNotificationsOn(self,conditionVariable):
        self.notifier.start(conditionVariable)

    def turnNotificationsOff(self):
        self.notifier.stop()

    def sendBuzz(self, message):
        buzz = Buzz(self.name, message)
        self.connectionManager.writeToQueue(config.dispatcherQueue(),buzz)
        logging.info("Sent buzz")
        return str(buzz.uId)

    def sendFollowHashtagPetition(self, hashtag):
        petition = FollowHashtagPetition(self.name, hashtag)
        self.connectionManager.writeToQueue(config.dispatcherQueue(),petition)
        logging.info("Sent follow hashtag petition")

    def sendFollowUserPetition(self, otherUser):
        petition = FollowUserPetition(self.name, otherUser)
        self.connectionManager.writeToQueue(config.dispatcherQueue(),petition)
        logging.info("Sent follow user petition")

    def sendRequestMessage(self, tag):
        request = QueryRequest(self.name,tag)
        self.connectionManager.writeToQueue(config.dispatcherQueue(),request)

    def sendDeleteMessage(self, uId):
        request = DeleteRequest(self.name,uId)
        self.connectionManager.writeToQueue(config.dispatcherQueue(),request)
        logging.info("Sent delete message")

    def requestTrendingTopics(self):
        request = TTRequest(self.name)
        self.connectionManager.writeToQueue(config.dispatcherQueue(),request)
        logging.info("Sent TT request")













