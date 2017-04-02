from ActionMessage import ActionMessage,FollowUserPetition,ShutdownSystemPetition
from Buzz import Buzz
from MessageUtils import MessageUtils
from User import User

name = raw_input("select name")
user = User(name)
user.startNotificationThread()

while(True):
    message = raw_input()
    values = message.split(' ')
    if(values[0] == "b"):
        user.sendBuzz(' '.join(values[1:]))
    elif(values[0] == 'fh'):
        user.sendFollowHashtagPetition(' '.join(values[1:]))
    elif(values[0] == 'fu'):
        user.sendFollowUserPetition(' '.join(values[1:]))
    elif(values[0] == 's'):
        user.sendShutdownPetition()
        break
    else:
        print "NADA"
        continue

user.stopNotificationThread()



