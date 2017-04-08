import threading
import logging
import time



from DBBuzzProcessingPool import DBBuzzProcessingPool
from DBIndexProcessingPool import DBIndexProcessingPool
from Dispatcher import Dispatcher
from User import User
from UserRegistrationHandler import UserRegistrationHandler

raw_input("Empezar sistema")
dispatcher = Dispatcher()
dispatcherThread = threading.Thread(target=dispatcher.start)
dispatcherThread.start()

index = DBIndexProcessingPool("*")
indexThread = threading.Thread(target=index.start)
indexThread.start()

db = DBBuzzProcessingPool("*")
dbThread = threading.Thread(target=db.start)
dbThread.start()

reg = UserRegistrationHandler()
regThread = threading.Thread(target=reg.waitForMessages)
regThread.start()

raw_input("Crear usuarios")
u1 = User("user1")
u1.turnNotificationsOn()
u2 = User("user2")
u2.turnNotificationsOn()
u3 = User("user3")
u3.turnNotificationsOn()

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




