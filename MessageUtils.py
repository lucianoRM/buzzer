import pickle

from InvalidMessageException import InvalidMessageException

'''Handles serialization and deserialization of messages'''

class MessageUtils:

    @classmethod
    def serialize(cls,messageObject):
        return pickle.dumps(messageObject)

    @classmethod
    def deserialize(cls,messageString):
        messageObject = None
        try:
            messageObject = pickle.loads(messageString)
        except:
            raise InvalidMessageException("The message received is not a known type")
        return messageObject