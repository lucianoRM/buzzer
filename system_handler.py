import signal

from db.DBBuzzProcessingPool import DBBuzzProcessingPool
from db.DBIndexProcessingPool import DBIndexProcessingPool
from listener.Dispatcher import Dispatcher
from listener.TTManager import TTManager
from listener.UserRegistrationHandler import UserRegistrationHandler
from utils.ThreadSafeVariable import ThreadSafeVariable

v = ThreadSafeVariable(True)

def stop(signal,frame):
    v.set(False)
    exit(0)

signal.signal(signal.SIGINT,stop)

dispatcher = Dispatcher()
dispatcher.start(v)

index = DBIndexProcessingPool(["#"])
index.start(v)

db = DBBuzzProcessingPool(["#"])
db.start(v)

reg = UserRegistrationHandler()
reg.start(v)

tt = TTManager()
tt.start(v)

print "System running, SIGINT to finish"

signal.pause()
