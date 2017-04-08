import threading
import logging

logging.getLogger("pika").setLevel(logging.WARNING)
from Buzz import Buzz
from ConnectionManager import ConnectionManager
from DBIndexManager import DBIndexManager
from DBRequest import DBRequest, QueryRequest
from MessageUtils import MessageUtils

POOL_SIZE = 10
FILE_KEY_LENGHT = 2
INCOMING_QUEUE_IP = 'localhost'
INCOMING_QUEUE_PORT = 5672
EXCHANGE_NAME = 'index-exchange'
QUEUE_NAME = 'index-queue'


class Worker:
    def run(self,semaphore,requestObject):
        logging.info("Worker started")
        manager = DBIndexManager(FILE_KEY_LENGHT)
        if(isinstance(requestObject,Buzz)):
            manager.storeIndex(requestObject)
        elif(isinstance(requestObject,QueryRequest)):
            manager.processRequest(requestObject)
        semaphore.release()




class DBIndexProcessingPool:

    def __init__(self,accessingKey):
        logging.getLogger(self.__class__.__name__)
        logging.basicConfig(filename="app.log",format='%(levelname)s:%(asctime)s:%(module)s@%(lineno)d:%(message)s', level=logging.INFO)
        self.accessingKey = accessingKey
        self.semaphore = threading.Semaphore(POOL_SIZE)
        self.incomingConnection = ConnectionManager(INCOMING_QUEUE_IP,INCOMING_QUEUE_PORT)
        self.incomingConnection.declareExchange(EXCHANGE_NAME)
        self.incomingConnection.declareQueue(QUEUE_NAME)
        self.incomingConnection.bindQueue(EXCHANGE_NAME,QUEUE_NAME,accessingKey)

    def processRequest(self, ch, method, properties, body):
        request = MessageUtils.deserialize(body)
        logging.info("Processing request")
        self.semaphore.acquire()
        worker = Worker()
        thread = threading.Thread(target=worker.run, args=(self.semaphore,request))
        thread.start()
        self.incomingConnection.ack(method.delivery_tag)

    def start(self):
        self.incomingConnection.listenToQueue(QUEUE_NAME,self.processRequest)



