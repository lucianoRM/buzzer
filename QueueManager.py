from MessageUtils import MessageUtils


class QueueManager:

    def __init__(self,connectionManager, queueName):
        self.connectionManager = connectionManager
        self.queueName = queueName
        self.connectionManager.channel.queue_declare(self.queueName)

    def writeToQueue(self, messageObject):
        message = messageObject
        if(not isinstance(messageObject,str)):
            message = MessageUtils.serialize(messageObject)
        self.connectionManager.channel.basic_publish('', self.queueName, message)

    def listenToQueue(self,callback):
        self.connectionManager.channel.basic_consume(callback, self.queueName)
        self.connectionManager.channel.start_consuming()

    def stopListeningToQueue(self):
        self.connectionManager.channel.stop_consuming()



