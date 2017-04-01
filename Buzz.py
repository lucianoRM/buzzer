import uuid
from InvalidMessageException import InvalidMessageException
import pickle

MAX_MESSAGE_LENGTH = 141


class Buzz(object):
    def __init__(self, message, user):
        if(len(message) > MAX_MESSAGE_LENGTH):
            raise InvalidMessageException("Message should be shorter than" + str(MAX_MESSAGE_LENGTH) + "characters")
        self.message = message
        self.user = user
        self.uId = uuid.uuid4()

    def __str__(self):
        return pickle.dumps(self)
