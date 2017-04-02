from ActionMessage import ShutdownSystemPetition,FollowUserPetition,FollowHashtagPetition
from Buzz import Buzz
from ConnectionManager import ConnectionManager
from MessageUtils import MessageUtils
import os

INCOMING_QUEUE_IP = 'localhost'
INCOMING_QUEUE_PORT = 5672
OUTGOING_QUEUE_IP = 'localhost'
OUTGOING_QUEUE_PORT = 5672
INCOMING_QUEUE_NAME = 'dispatcher-registrationhandler'
REGISTRATION_FOLDER = './reg'
USERS_INFO_FOLDER = './reg/users_info'



'''Persists registrations to hashtags and other users'''
class UserRegistrationHandler:



    def __init__(self):
        self.incomingConnectionManager = ConnectionManager(INCOMING_QUEUE_IP,INCOMING_QUEUE_PORT)
        self.outgoingConnectionManager = ConnectionManager(OUTGOING_QUEUE_IP,OUTGOING_QUEUE_PORT)
        self.incomingConnectionManager.declareQueue(INCOMING_QUEUE_NAME)
        self.initializeUserQueues()

    def initializeUserQueues(self):
        try:
            for user in os.listdir(USERS_INFO_FOLDER):
                self.incomingConnectionManager.declareQueue(user)
        except:
            print "Folder path not configured for user registration"


    def updateUserRegistrations(self,user,registrationTarget):
        filename = USERS_INFO_FOLDER + '/' + user
        try:
            file = open(filename, 'a+r')
        except IOError:
            print "Folder path not configured for user registration"
        for line in file:
            if (line.strip() == registrationTarget):
                file.close()
                return False  # User was already registered in target
        file.write(registrationTarget + '\n')
        file.close()
        return True



    '''The registration target can be a Hashtag or another User'''
    def register(self, user, registrationTarget):
        shouldUpdate = self.updateUserRegistrations(user,registrationTarget)
        if(shouldUpdate):
            filename = REGISTRATION_FOLDER + '/' + registrationTarget
            file = open(filename, 'a+r')
            file.write(user + '\n')
            file.close()


    def getFollowers(self,target):
        filename = REGISTRATION_FOLDER + '/' + target
        followers = []
        try:
            file = open(filename,'r')
            for line in file:
                followers.append(line.strip())
        except IOError:
            #If here is because target does not have any follower so nothing should be done
            return followers
        return followers


    def notifyFollowers(self,buzz):
        hashtags = buzz.getHashtags()
        buzzer = buzz.user
        followers = []
        followers += self.getFollowers(buzzer)
        for hashtag in hashtags:
            followers += self.getFollowers(hashtag)
        print followers
        for follower in list(set(followers)):
            if(follower != buzzer): #to avoid sending message to it's own
                self.outgoingConnectionManager.writeToQueue(follower, buzz)


    def onMessageReceived(self, channel, method, properties, body):

        message = MessageUtils.deserialize(body)
        if(isinstance(message, FollowUserPetition)):
            print "FollowUserPetition"
            self.register(message.user,message.otherUser)
        elif(isinstance(message, FollowHashtagPetition)):
            print "FollowHashtagPetition"
            self.register(message.user,message.hashtag)
        elif(isinstance(message, Buzz)):
            print "Buzz send to"
            self.notifyFollowers(message)
        elif(isinstance(message, ShutdownSystemPetition)):
            self.incomingConnectionManager.ack(method.delivery_tag)
            self.terminate()
            return
        self.incomingConnectionManager.ack(method.delivery_tag)

    def waitForMessages(self):
        self.incomingConnectionManager.listenToQueue(INCOMING_QUEUE_NAME,self.onMessageReceived)
        "Finished!"

    def terminate(self):
        self.incomingConnectionManager.stopListeningToQueue()
        self.outgoingConnectionManager.close()
        self.incomingConnectionManager.close()

urh = UserRegistrationHandler()
urh.waitForMessages()