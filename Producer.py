from ActionMessage import ActionMessage,FollowUserPetition,ShutdownSystemPetition
from Buzz import Buzz
from MessageUtils import MessageUtils
from User import User

user = User('luciano')

while(True):
    message = raw_input()
    if(message == "b"):
        action = Buzz('message','luciano')
    elif(message == 'f'):
        action = FollowUserPetition('luciano', 'pedro')
    elif(message == 's'):
        action = ShutdownSystemPetition('luciano')
    else:
        print "NADA"
        continue

    user.send(MessageUtils.serialize(action))



