import random
import signal
import threading
import time
from listener.Dispatcher import Dispatcher
from db.DBBuzzProcessingPool import DBBuzzProcessingPool
from db.DBIndexProcessingPool import DBIndexProcessingPool
from listener.TTManager import TTManager
from listener.UserRegistrationHandler import UserRegistrationHandler
from user.User import User
from utils.ThreadSafeVariable import ThreadSafeVariable


CONCURRENT_USERS = 10
USER_WAITING_TIME = 0.1


v = ThreadSafeVariable(True)

def stop(signal,frame):
    v.set(False)
    exit(0)

signal.signal(signal.SIGINT,stop)

wordList = ['hola','como','estas','perro','gato','vaca','ballena','raton','pelota','rojo','verde',
            'azul','computadora','juego','comida']

hashtagOrNot = ['#','']

def run(id,v):
    u = User(str(id))
    while(v.get()):
        message = ''
        for i in xrange(random.randint(1,5)):
            message += ' ' + random.choice(hashtagOrNot) + random.choice(wordList)
        u.sendBuzz(message)
        time.sleep(USER_WAITING_TIME)

raw_input("Crear usuarios")
for i in xrange(CONCURRENT_USERS):
     t = threading.Thread(target=run, args=(i,v))
     t.start()


name = raw_input("select name: ")
user = User(name)
user.turnNotificationsOn(v)

while(True):
    message = raw_input("cmd: ")
    values = message.split(' ')
    if(values[0] == "b"):
        user.sendBuzz(' '.join(values[1:]))
    elif(values[0] == 'fh'):
        user.sendFollowHashtagPetition(' '.join(values[1:]))
    elif(values[0] == 'fu'):
        user.sendFollowUserPetition(' '.join(values[1:]))
    elif(values[0] == 'd'):
        user.sendDeleteMessage(values[1])
    elif(values[0] == 'r'):
        user.sendRequestMessage(values[1])
    elif(values[0] == 'tt'):
        user.requestTrendingTopics()
    elif(values[0] == 'exit'):
        print "Sali!, esperando por SIGINT"
        break

signal.pause()

