from ActionMessage import ShutdownSystemPetition,FollowUserPetition,FollowHashtagPetition
from Buzz import Buzz
from ConnectionManager import ConnectionManager
from MessageUtils import MessageUtils
from QueueManager import QueueManager

INCOMING_QUEUE_IP = 'localhost'
INCOMING_QUEUE_PORT = 5672
OUTGOING_QUEUE_IP = 'localhost'
OUTGOING_QUEUE_PORT = 5672
INCOMING_QUEUE_NAME = 'dispatcher-registrationhandler'
REGISTRATION_FOLDER = './reg'
USERS_INFO_FOLDER = './reg/users_info'

class UserRegistrationHandler:



    def __init__(self):
        self.incomingConnectionManager = ConnectionManager(INCOMING_QUEUE_IP,INCOMING_QUEUE_PORT)
        self.outgoingConnectionManager = ConnectionManager(OUTGOING_QUEUE_IP,OUTGOING_QUEUE_PORT)
        self.incomingQueueManager = QueueManager(self.incomingConnectionManager, INCOMING_QUEUE_NAME)


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
        print list(set(followers))


    def onMessageReceived(self, channel, method, properties, body):
        try:
            message = MessageUtils.deserialize(body)
        except:
            print "Error deserializing"
        if(isinstance(message, FollowUserPetition)):
            print "Is follow user"
        elif(isinstance(message, Buzz)):
            print "Is Buzz"
        elif(isinstance(message, ShutdownSystemPetition)):
            self.incomingConnectionManager.channel.basic_ack(method.delivery_tag)
            self.terminate()
            return

        self.incomingConnectionManager.channel.basic_ack(method.delivery_tag)

    def waitForMessages(self):
        self.incomingQueueManager.listenToQueue(self.onMessageReceived)
        "Finished!"

    def terminate(self):
        self.incomingQueueManager.stopListeningToQueue()
        self.outgoingConnectionManager.close()
        self.incomingConnectionManager.close()

urh = UserRegistrationHandler()
urh.waitForMessages()