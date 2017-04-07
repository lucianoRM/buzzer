import threading

from ConnectionManager import ConnectionManager

POOL_SIZE = 10
INCOMING_QUEUE_IP = 'localhost'
INCOMING_QUEUE_PORT = 5672
EXCHANGE_NAME = 'e'
QUEUE_NAME = 'queue'


class Worker:
    def run(self,semaphore,request):

        semaphore.release()




class DBIndexProcessingPool:

    def __init__(self,accessingKey):
        self.accessingKey = accessingKey
        self.semaphore = threading.Semaphore(POOL_SIZE)
        self.incomingConnection = ConnectionManager(INCOMING_QUEUE_IP,INCOMING_QUEUE_PORT)
        self.incomingConnection.declareExchange(EXCHANGE_NAME)
        self.incomingConnection.declareQueue(QUEUE_NAME)
        self.incomingConnection.bindQueue(EXCHANGE_NAME,QUEUE_NAME,accessingKey)

    def method(self,ch, method, properties, body):
        print body

    def start(self):
        self.incomingConnection.listenToQueue(QUEUE_NAME,self.method)

    def run(self):
        for i in xrange(20):
            self.semaphore.acquire()
            worker = Worker()
            thread = threading.Thread(target=worker.run,args=(self.semaphore,))
            t.start()


a = DBIndexProcessingPool("a.*")
a.start()

