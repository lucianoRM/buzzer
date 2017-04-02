import uuid

import pickle


class ActionMessage(object):

    def __init__(self, user):
        self.user = user
        self.uId = uuid.uuid4()

    def __str__(self):
        return pickle.dumps(self)

    def createFollowUserPetition(self, otherUser):
        self.data = otherUser
        self.action = self.FOLLOW_USER

    def createFollowHashtagPetition(self, hashtag):
        self.data = hashtag
        self.action = self.FOLLOW_HASHTAG

    def createSystemShutdownPetition(self):
        self.action = self.SHUTDOWN_SYSTEM


class FollowUserPetition(ActionMessage):
    def __init__(self,me, otherUser):
        super(FollowUserPetition, self).__init__(me)
        self.otherUser = otherUser

class FollowHashtagPetition(ActionMessage):

    def __init__(self,user, hashtag):
        super(FollowHashtagPetition, self).__init__(user)
        self.hashtag = hashtag


class ShutdownSystemPetition(ActionMessage):

    def __init__(self, user):
        super(ShutdownSystemPetition, self).__init__(user)
