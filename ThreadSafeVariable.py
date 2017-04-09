import threading

class ThreadSafeVariable:


    def __init__(self,value= None):
        self.value = value
        self.lock = threading.Lock()

    def get(self):
        self.lock.acquire()
        value = self.value
        self.lock.release()
        return value

    def set(self,value):
        self.lock.acquire()
        self.value = value
        self.lock.release()
