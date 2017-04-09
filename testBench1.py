import threading
import logging
import time

import signal

from DBBuzzProcessingPool import DBBuzzProcessingPool
from DBIndexProcessingPool import DBIndexProcessingPool
from Dispatcher import Dispatcher
from ThreadSafeVariable import ThreadSafeVariable
from User import User
from UserRegistrationHandler import UserRegistrationHandler

v = ThreadSafeVariable(True)

def stop(signal,frame):
    v.set(False)
    exit(0)

signal.signal(signal.SIGINT,stop)

raw_input("Empezar sistema")
dispatcher = Dispatcher()
dispatcher.start(v)

index = DBIndexProcessingPool(["*"])
index.start(v)

db = DBBuzzProcessingPool(["*"])
db.start(v)


reg = UserRegistrationHandler()
reg.start(v)

raw_input("Crear usuarios")
u1 = User("user1")
u1.turnNotificationsOn(v)
u2 = User("user2")
u2.turnNotificationsOn(v)
u3 = User("user3")
u3.turnNotificationsOn(v)

u2.sendFollowUserPetition("user1")
u3.sendFollowUserPetition("user1")
u1.sendFollowHashtagPetition("#hello")

raw_input("user1 emitir mensaje")
u1.sendBuzz("hola, soy user1")

time.sleep(1)
raw_input("user2 emitir mensaje")
u2.sendBuzz("hola soy user2")

time.sleep(1)
raw_input("user3 emitir mensaje con hashtag")
u3.sendBuzz("hola soy user3 mando #hello")

time.sleep(1)
raw_input("user2 emitir otro mensaje")
id = u2.sendBuzz("soy user2, alguien me escucha?")

time.sleep(1)
raw_input("user1 consultar mensajes de user2")
u1.sendRequestMessage("user2")

time.sleep(1)
raw_input("user2 borrar 1 mensaje")
u2.sendDeleteMessage(id)

time.sleep(1)
raw_input("user1 volver a checkear mensajes de user2")
u1.sendRequestMessage("user2")





signal.pause()
