import Queue
import logging
import threading

from config import config
from listener.GenericListener import GenericListener
from utils.ThreadSafeVariable import ThreadSafeVariable

logging.getLogger("pika").setLevel(logging.WARNING)


from messages.Buzz import Buzz
from db.DBBuzzManager import DBBuzzManager
from DBRequest import QueryRequest, DeleteRequest
from utils.MessageUtils import MessageUtils


class Worker:

    def run(self,queue,shouldRun):
        while(shouldRun.get()):
            logging.info("Processing request")
            try:
                requestObject = queue.get(timeout=config.timeout())
            except:
                continue
            manager = DBBuzzManager(config.buzzKeyLength())
            if (isinstance(requestObject, Buzz)):
                manager.store(requestObject)
            elif (isinstance(requestObject, QueryRequest)):
                manager.processRequest(requestObject)
            elif (isinstance(requestObject, DeleteRequest)):
                manager.deleteBuzz(requestObject)




class DBBuzzProcessingPool(GenericListener):

    def __init__(self,accessingKeys):
        GenericListener.__init__(self,config.ip(),config.port())
        self.incomingConnectionManager.declareExchange(config.dbExchange())
        self.queueName = self.incomingConnectionManager.declareQueue()
        for accessingKey in accessingKeys:
            self.incomingConnectionManager.bindQueue(config.dbExchange(),self.queueName,accessingKey)
        self.threadQueue = Queue.Queue()
        self.pool = [threading.Thread(target=Worker().run, args=(self.threadQueue,self.v)).start() for i in xrange(config.poolSize())]

    def processRequest(self,ch, method, properties, body):
        request = MessageUtils.deserialize(body)
        logging.info("Request received")
        self.threadQueue.put(request)
        self.incomingConnectionManager.ack(method.delivery_tag)
        if not self.keepRunning.get():
            self.v.set(False)
            self.stop()

    def _start(self):
        self.incomingConnectionManager.addTimeout(self.onTimeout)
        self.incomingConnectionManager.listenToQueue(self.queueName,self.processRequest)






