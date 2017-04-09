import threading
import logging

from GenericListener import GenericListener

logging.getLogger("pika").setLevel(logging.WARNING)


from Buzz import Buzz
from ConnectionManager import ConnectionManager
from DBBuzzManager import DBBuzzManager
from DBIndexManager import DBIndexManager
from DBRequest import DBRequest, QueryRequest, DeleteRequest
from MessageUtils import MessageUtils

POOL_SIZE = 10
FILE_KEY_LENGHT = 1
INCOMING_QUEUE_IP = 'localhost'
INCOMING_QUEUE_PORT = 5672
EXCHANGE_NAME = 'buzz-exchange'
QUEUE_NAME = 'buzz-queue'


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

    def __init__(self,accessingKey):
        GenericListener.__init__(self,INCOMING_QUEUE_IP,INCOMING_QUEUE_PORT)
        self.accessingKey = accessingKey
        self.semaphore = threading.Semaphore(POOL_SIZE)
        self.incomingConnectionManager.declareExchange(EXCHANGE_NAME)
        self.incomingConnectionManager.declareQueue(QUEUE_NAME)
        self.incomingConnectionManager.bindQueue(EXCHANGE_NAME,QUEUE_NAME,accessingKey)

    def processRequest(self,ch, method, properties, body):
        request = MessageUtils.deserialize(body)
        logging.info("Request received")
        self.semaphore.acquire()
        worker = Worker()
        thread = threading.Thread(target=worker.run, args=(self.semaphore,request))
        thread.start()
        self.incomingConnectionManager.ack(method.delivery_tag)

    def _start(self):
        self.incomingConnectionManager.addTimeout(self.onTimeout)
        self.incomingConnectionManager.listenToQueue(QUEUE_NAME,self.processRequest)




