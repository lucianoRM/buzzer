import threading

from Buzz import Buzz
from ConnectionManager import ConnectionManager
from DBBuzzManager import DBBuzzManager
from DBIndexManager import DBIndexManager
from DBRequest import DBRequest, QueryRequest, DeleteRequest
from MessageUtils import MessageUtils

POOL_SIZE = 10
FILE_KEY_LENGHT = 2
INCOMING_QUEUE_IP = 'localhost'
INCOMING_QUEUE_PORT = 5672
EXCHANGE_NAME = 'buzz-exchange'
QUEUE_NAME = 'queue'


class Worker:
    def run(self,semaphore,requestObject):
        manager = DBBuzzManager(FILE_KEY_LENGHT)
        if(isinstance(requestObject,Buzz)):
            manager.store(requestObject)
        elif(isinstance(requestObject,QueryRequest)):
            manager.processRequest(requestObject)
        elif(isinstance(requestObject,DeleteRequest)):
            manager.deleteBuzz(requestObject)
        semaphore.release()




class DBIndexProcessingPool:

    def __init__(self,accessingKey):
        self.accessingKey = accessingKey
        self.semaphore = threading.Semaphore(POOL_SIZE)
        self.incomingConnection = ConnectionManager(INCOMING_QUEUE_IP,INCOMING_QUEUE_PORT)
        self.incomingConnection.declareExchange(EXCHANGE_NAME)
        self.incomingConnection.declareQueue(QUEUE_NAME)
        self.incomingConnection.bindQueue(EXCHANGE_NAME,QUEUE_NAME,accessingKey)

    def processRequest(self,ch, method, properties, body):
        request = MessageUtils.deserialize(body)
        self.semaphore.acquire()
        worker = Worker()
        thread = threading.Thread(target=worker.run, args=(self.semaphore,request))
        thread.start()

    def start(self):
        self.incomingConnection.listenToQueue(QUEUE_NAME,self.processRequest)





a = DBIndexProcessingPool("*")
a.start()

