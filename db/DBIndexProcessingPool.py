import Queue
import logging
import threading

from listener.GenericListener import GenericListener
from utils.ThreadSafeVariable import ThreadSafeVariable

logging.getLogger("pika").setLevel(logging.WARNING)
from messages.Buzz import Buzz
from db.DBIndexManager import DBIndexManager
from DBRequest import QueryRequest
from utils.MessageUtils import MessageUtils

POOL_SIZE = 10
FILE_KEY_LENGHT = 2
INCOMING_QUEUE_IP = 'localhost'
INCOMING_QUEUE_PORT = 5672
EXCHANGE_NAME = 'index-exchange'
TIMEOUT=3



class Worker:
    def run(self,queue,shouldRun):
        while(shouldRun.get()):
            logging.info("Processing request")
            try:
                requestObject = queue.get(timeout=TIMEOUT)
            except:
                continue
            manager = DBIndexManager(FILE_KEY_LENGHT)
            if (isinstance(requestObject, Buzz)):
                manager.storeIndex(requestObject)
            elif (isinstance(requestObject, QueryRequest)):
                manager.processRequest(requestObject)




class DBIndexProcessingPool(GenericListener):

    def __init__(self,accessingKeys):
        GenericListener.__init__(self,INCOMING_QUEUE_IP,INCOMING_QUEUE_PORT)
        self.incomingConnectionManager.declareExchange(EXCHANGE_NAME)
        self.queueName = self.incomingConnectionManager.declareQueue()
        for accessingKey in accessingKeys:
            self.incomingConnectionManager.bindQueue(EXCHANGE_NAME,self.queueName,accessingKey)
        self.threadQueue = Queue.Queue()
        self.pool = [threading.Thread(target=Worker().run, args=(
        self.threadQueue, self.v)).start() for i in xrange(POOL_SIZE)]

    def processRequest(self, ch, method, properties, body):
        request = MessageUtils.deserialize(body)
        logging.info("Processing request")
        self.threadQueue.put(request)
        self.incomingConnectionManager.ack(method.delivery_tag)
        if not self.keepRunning.get():
            self.v.set(False)
            self.stop()

    def _start(self):
        self.incomingConnectionManager.addTimeout(self.onTimeout)
        self.incomingConnectionManager.listenToQueue(self.queueName,self.processRequest)



