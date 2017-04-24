import abc
import logging
import threading

from connection.ConnectionManager import ConnectionManager
from utils.ThreadSafeVariable import ThreadSafeVariable


class GenericListener:

    def __init__(self, incomingConnectionIP, incomingConnectionPort):
        __metaclass__ = abc.ABCMeta
        logging.basicConfig(filename="app.log", format='%(levelname)s:%(asctime)s:%(module)s@%(lineno)d:%(message)s',level=logging.INFO)
        self.incomingConnectionManager = ConnectionManager(incomingConnectionIP, incomingConnectionPort)
        self.listeningThread = None
        self.v = ThreadSafeVariable(True)

    def onTimeout(self):
        if (not self.keepRunning.get()):
            self.v.set(False)
            self.stop()
            return
        self.incomingConnectionManager.addTimeout(self.onTimeout)

    @abc.abstractmethod
    def _start(self):

        raise NotImplementedError("Should implement _start()")


    def start(self,conditionVariable):
        self.listeningThread = threading.Thread(target=self._start)
        self.keepRunning = conditionVariable
        self.listeningThread.start()

    def stop(self):
        self.incomingConnectionManager.stopListeningToQueue()
        self.incomingConnectionManager.close()






