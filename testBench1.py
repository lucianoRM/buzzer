import signal
import time

from user.User import User
from utils.ThreadSafeVariable import ThreadSafeVariable

v = ThreadSafeVariable(True)

def stop(signal,frame):
    v.set(False)
    exit(0)

signal.signal(signal.SIGINT,stop)

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

time.sleep(1)
raw_input("user2 pedi los TT")
u2.requestTrendingTopics()

time.sleep(1)
raw_input("Generar mas TT")
u1.sendBuzz("Soy #user1 y digo: 1")
u1.sendBuzz("Soy #user1 y digo: 2")
u1.sendBuzz("Soy #user1 y digo: 3")
u1.sendBuzz("Soy #user1 y digo: 4")
u2.sendBuzz("Soy #user2 y digo: 1")
u2.sendBuzz("Soy #user2 y digo: 2")
u2.sendBuzz("Soy #user2 y digo: 3")
u2.sendBuzz("Soy #user2 y digo: 4")
u3.sendBuzz("Soy #user3 y digo: 1")
u3.sendBuzz("Soy #user3 y digo: 2")
u3.sendBuzz("Soy #user3 y digo: 3")
u3.sendBuzz("Soy #user3 y digo: 4")

time.sleep(1)
raw_input("user2 pedi los TT")
u2.requestTrendingTopics()

signal.pause()
