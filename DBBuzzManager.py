import fcntl

from Buzz import Buzz

ROOT_PATH = './buzz'

'''Handles db files and stores information'''
class DBBuzzManager:


    def __init__(self,keyLength):
        self.keyLength = keyLength

    def createInfoEntry(self,buzz):
        argsList = []
        argsList.append('+') # + means that the buzz is accesible
        argsList.append(str(buzz.uId))
        argsList.append(buzz.user)
        argsList.append(buzz.message)
        return ";".join(argsList)

    def getFileKey(self,uId):
        fileKey = uId[:self.keyLength]
        return fileKey

    def getFilePath(self, uId):
        return ROOT_PATH + "/" + self.getFileKey(uId)

    def store(self,buzz):
        filename = self.getFilePath(str(buzz.uId))
        file = open(filename, 'a+')
        fcntl.flock(file,fcntl.LOCK_EX)
        file.write(self.createInfoEntry(buzz))
        fcntl.flock(file, fcntl.LOCK_UN)
        file.close()

    def retrieveBuzz(self, uId):
        uId = str(uId)
        filename = self.getFilePath(uId)
        file = open(filename, 'r')
        buzz = None
        fcntl.flock(file, fcntl.LOCK_SH)
        for line in file:
            args = line.split(";")
            if(args[0] == "+" and args[1] == uId):
                buzz = ";".join(args[1:])
                break
        fcntl.flock(file, fcntl.LOCK_UN)
        file.close()
        return buzz

    def deleteBuzz(self, uId):
        uId = str(uId)
        filename = self.getFilePath(uId)
        file = open(filename, 'r+')
        fcntl.flock(file, fcntl.LOCK_EX)
        location = 0
        found = False
        for line in file:
            args = line.split(";")
            if (args[0] == "+" and args[1] == uId):
                found = True
                break
            location+=len(line)
        if(found):
            file.seek(location)
            file.write("-")
        fcntl.flock(file, fcntl.LOCK_UN)
        file.close()



db = DBBuzzManager(3)

a = Buzz('luciano', 'Este es el mensaje')
db.store(a)
print db.retrieveBuzz(a.uId)
db.deleteBuzz(a.uId)
print db.retrieveBuzz(a.uId)




