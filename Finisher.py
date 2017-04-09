import signal

class Finisher:

    def __init__(self,connectionManager):
        self.connectionManager = connectionManager
        signal.signal(signal.SIGINT, self.onSignal)


    def start(self):
        signal.pause()


    def onSignal(self,signal,frame):
        print "Signaled!"
        self.connectionManager.close()

