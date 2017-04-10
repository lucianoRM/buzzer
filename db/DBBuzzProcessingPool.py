import logging
import threading

from listener.GenericListener import GenericListener

logging.getLogger("pika").setLevel(logging.WARNING)


from messages.Buzz import Buzz
from db.DBBuzzManager import DBBuzzManager
from DBRequest import QueryRequest, DeleteRequest
from utils.MessageUtils import MessageUtils

POOL_SIZE = 10
FILE_KEY_LENGHT = 1
INCOMING_QUEUE_IP = 'localhost'
INCOMING_QUEUE_PORT = 5672
EXCHANGE_NAME = 'buzz-exchange'


class Worker:
    def run(self,semaphore,requestObject):
        logging.info("Processing request")
        manager = DBBuzzManager(FILE_KEY_LENGHT)
        if(isinstance(requestObject,Buzz)):
            manager.store(requestObject)
        elif(isinstance(requestObject,QueryRequest)):
            manager.processRequest(requestObject)
        elif(isinstance(requestObject,DeleteRequest)):
            manager.deleteBuzz(requestObject)
        logging.info("Releasing semaphore")
        semaphore.release()




class DBBuzzProcessingPool(GenericListener):

    def __init__(self,accessingKeys):
        GenericListener.__init__(self,INCOMING_QUEUE_IP,INCOMING_QUEUE_PORT)
        self.semaphore = threading.Semaphore(POOL_SIZE)
        self.incomingConnectionManager.declareExchange(EXCHANGE_NAME)
        self.queueName = self.incomingConnectionManager.declareQueue()
        for accessingKey in accessingKeys:
            self.incomingConnectionManager.bindQueue(EXCHANGE_NAME,self.queueName,accessingKey)

    def processRequest(self,ch, method, properties, body):
        request = MessageUtils.deserialize(body)
        logging.info("Request received")
        self.semaphore.acquire()
        worker = Worker()
        thread = threading.Thread(target=worker.run, args=(self.semaphore,request))
        thread.start()
        self.incomingConnectionManager.ack(method.delivery_tag)
        if not self.keepRunning.get():
            self.stop()

    def _start(self):
        self.incomingConnectionManager.addTimeout(self.onTimeout)
        self.incomingConnectionManager.listenToQueue(self.queueName,self.processRequest)






