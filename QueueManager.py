
class QueueManager:

    def __init__(self,connectionManager, queueName):
        self.connectionManager = connectionManager
        self.queueName = queueName
        self.connectionManager.channel.queue_declare(self.queueName)

    def writeToQueue(self, buzz):
        self.connectionManager.channel.basic_publish('', self.queueName, buzz)

    def listenToQueue(self,callback):
        self.connectionManager.channel.basic_consume(callback, self.queueName)
        self.connectionManager.channel.start_consuming()

    def stopListeningToQueue(self):
        self.connectionManager.channel.stop_consuming()



