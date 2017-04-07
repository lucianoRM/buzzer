import pika

from MessageUtils import MessageUtils


class ConnectionManager:

    def __init__(self,ip,port):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(ip, port))
        self.channel = self.connection.channel()

    def declareQueue(self, queueName):
        self.channel.queue_declare(queueName)

    def writeToQueue(self, queueName, messageObject):
        message = messageObject
        if (not isinstance(messageObject, str)):
            message = MessageUtils.serialize(messageObject)
        self.channel.basic_publish('', queueName, message)

    def listenToQueue(self, queueName, callback):
        self.channel.basic_consume(callback, queueName)
        self.channel.start_consuming()

    def stopListeningToQueue(self):
        self.channel.stop_consuming()

    def ack(self,tag):
        self.channel.basic_ack(tag)

    def declareExchange(self,name):
        self.channel.exchange_declare(exchange=name,type='topic')

    def bindQueue(self,exchangeName,queueName,pattern):
        self.channel.queue_bind(exchange=exchangeName,queue=queueName,routing_key=pattern)

    def close(self):
        self.connection.close()


