import fcntl
import logging
import os

from connection.ConnectionManager import ConnectionManager
from messages.ActionMessage import FollowUserPetition, FollowHashtagPetition
from messages.Buzz import Buzz
from listener.GenericListener import GenericListener
from utils.MessageUtils import MessageUtils

INCOMING_QUEUE_IP = 'localhost'
INCOMING_QUEUE_PORT = 5672
OUTGOING_QUEUE_IP = 'localhost'
OUTGOING_QUEUE_PORT = 5672
INCOMING_QUEUE_NAME = 'dispatcher-registrationhandler'
REGISTRATION_FOLDER = './reg'
USERS_INFO_FOLDER = './reg/users_info'



'''Persists registrations to hashtags and other users'''
class UserRegistrationHandler(GenericListener):



    def __init__(self):

        GenericListener.__init__(self,INCOMING_QUEUE_IP,INCOMING_QUEUE_PORT)
        self.outgoingConnectionManager = ConnectionManager(OUTGOING_QUEUE_IP,OUTGOING_QUEUE_PORT)
        self.incomingConnectionManager.declareQueue(INCOMING_QUEUE_NAME)
        self.initializeUserQueues()

    def initializeUserQueues(self):
        try:
            for user in os.listdir(USERS_INFO_FOLDER):
                self.incomingConnectionManager.declareQueue(user)
        except:
            logging.error("Folder path not configured for user registration")


    def updateUserRegistrations(self,user,registrationTarget):
        filename = USERS_INFO_FOLDER + '/' + user
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
            filename = REGISTRATION_FOLDER + '/' + registrationTarget
            file = open(filename, 'a+r')
            fcntl.flock(file, fcntl.LOCK_EX) #lock file for writing
            file.write(user + '\n')
            fcntl.flock(file, fcntl.LOCK_UN)
            file.close()


    def getFollowers(self,target):
        filename = REGISTRATION_FOLDER + '/' + target
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



    def _start(self):
        self.incomingConnectionManager.addTimeout(self.onTimeout)
        self.incomingConnectionManager.listenToQueue(INCOMING_QUEUE_NAME, self.onMessageReceived)





