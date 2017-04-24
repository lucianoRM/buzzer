import fcntl
import logging
import os

from config import config
from connection.ConnectionManager import ConnectionManager
from messages.ActionMessage import FollowUserPetition, FollowHashtagPetition
from messages.Buzz import Buzz
from listener.GenericListener import GenericListener
from utils.MessageUtils import MessageUtils




'''Persists registrations to hashtags and other users'''
class UserRegistrationHandler(GenericListener):



    def __init__(self):

        GenericListener.__init__(self,config.ip(),config.port())
        self.outgoingConnectionManager = ConnectionManager(config.ip(),config.port())
        self.incomingConnectionManager.declareQueue(config.registrationQueueName())
        self.initializeUserQueues()

    def initializeUserQueues(self):
        try:
            for user in os.listdir(config.registrationUsersInfoFolder()):
                self.incomingConnectionManager.declareQueue(user)
        except:
            logging.error("Folder path not configured for user registration")


    def updateUserRegistrations(self,user,registrationTarget):
        filename = config.registrationUsersInfoFolder() + '/' + user
        try:
            file = open(filename, 'a+r')
            fcntl.flock(file, fcntl.LOCK_EX) #Lock file for writing
        except IOError:
            logging.error("Folder path not configured for user registration")
        for line in file:
            if (line.strip() == registrationTarget):
                fcntl.flock(file, fcntl.LOCK_UN) #unlock file
                file.close()
                return False  # User was already registered in target
        file.write(registrationTarget + '\n')
        fcntl.flock(file, fcntl.LOCK_UN) #unlock file
        file.close()
        return True



    '''The registration target can be a Hashtag or another User'''
    def register(self, user, registrationTarget):
        shouldUpdate = self.updateUserRegistrations(user,registrationTarget)
        if(shouldUpdate):
            filename = config.registrationFolder() + '/' + registrationTarget
            file = open(filename, 'a+r')
            fcntl.flock(file, fcntl.LOCK_EX) #lock file for writing
            file.write(user + '\n')
            fcntl.flock(file, fcntl.LOCK_UN)
            file.close()


    def getFollowers(self,target):
        filename = config.registrationFolder() + '/' + target
        followers = []
        try:
            file = open(filename,'r')
            fcntl.flock(file, fcntl.LOCK_SH)
            for line in file:
                followers.append(line.strip())
        except IOError:
            #If here is because target does not have any follower so nothing should be done
            return followers
        fcntl.flock(file, fcntl.LOCK_UN)
        file.close()
        return followers


    def notifyFollowers(self,buzz):
        hashtags = buzz.getHashtags()
        buzzer = buzz.user
        followers = []
        followers += self.getFollowers(buzzer)
        for hashtag in hashtags:
            followers += self.getFollowers(hashtag)
        for follower in list(set(followers)):
            if(follower != buzzer): #to avoid sending message to it's own
                self.outgoingConnectionManager.writeToQueue(follower, buzz)


    def onMessageReceived(self, channel, method, properties, body):

        message = MessageUtils.deserialize(body)
        if(isinstance(message, FollowUserPetition)):
            logging.info("processing follow user petition")
            self.register(message.user,message.otherUser)
        elif(isinstance(message, FollowHashtagPetition)):
            logging.info("processing follow hashtag petition")
            self.register(message.user,message.hashtag)
        elif(isinstance(message, Buzz)):
            logging.info("processed buzz")
            self.notifyFollowers(message)
        self.incomingConnectionManager.ack(method.delivery_tag)
        if not self.keepRunning.get():
            self.stop()



    def _start(self):
        self.incomingConnectionManager.addTimeout(self.onTimeout)
        self.incomingConnectionManager.listenToQueue(config.registrationQueueName(), self.onMessageReceived)





