import pika

from utils.MessageUtils import MessageUtils

CONNECTION_TIMEOUT=3

class ConnectionManager:

    def __init__(self,ip,port):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(ip, port))
        self.channel = self.connection.channel()

    def declareQueue(self, queueName=None):
        if(not queueName):
            result = self.channel.queue_declare()
            return result.method.queue
        self.channel.queue_declare(queueName)

    def writeToQueue(self, queueName, messageObject):
        message = messageObject
        if (not isinstance(messageObject, str)):
            message = MessageUtils.serialize(messageObject)
        self.channel.basic_publish('', queueName, message)

    def writeToExchange(self,exchangeName,key,messageObject):
        message = messageObject
        if (not isinstance(messageObject, str)):
            message = MessageUtils.serialize(messageObject)
        self.channel.basic_publish(exchange=exchangeName,routing_key=".".join(list(str(key))), body= message)

    def listenToQueue(self, queueName, callback):
        self.channel.basic_consume(callback, queueName)
        self.channel.start_consuming()

    def addTimeout(self,timeoutCallback):
        self.connection.add_timeout(CONNECTION_TIMEOUT, timeoutCallback)

    def stopListeningToQueue(self):
        self.channel.stop_consuming()

    def ack(self,tag):
        self.channel.basic_ack(tag)

    def declareExchange(self,name):
        self.channel.exchange_declare(exchange=name,type='topic')

    def bindQueue(self,exchangeName,queueName,pattern):
        self.channel.queue_bind(exchange=exchangeName,queue=queueName,routing_key=pattern)

    def close(self):
        self.channel.cancel()
        self.connection.close()


