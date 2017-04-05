import uuid
import re
from InvalidMessageException import InvalidMessageException

MAX_MESSAGE_LENGTH = 141


class Buzz(object):

    def __init__(self, user, message, uId = None):
        if(len(message) > MAX_MESSAGE_LENGTH):
            raise InvalidMessageException("Message should be shorter than" + str(MAX_MESSAGE_LENGTH) + "characters")
        self.message = message
        self.user = user
        if(uId == None):
            self.uId = uuid.uuid4()
        else:
            self.uId = uId

    def getHashtags(self):
        return re.findall(r'(?i)\#\w+', self.message)

