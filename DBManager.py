from Buzz import Buzz

ROOT_PATH = './db'
USERS_PATH = './db/users' #stores a file for every user with all buzzes from that user
HASHTAGS_PATH = './db/hashtags' #stores a file for every hashtag with all the buzzes containing that hashtag
INDEX_PATH = './db/index' #stores every saved buzz id and path for deleting

'''Handles db files and stores information'''
class DBManager:

    def createInfoEntry(self,buzz):
        argsList = []
        argsList.append(str(buzz.uId))
        argsList.append(buzz.user)
        argsList.append(buzz.message)
        return ";".join(argsList)

    def createIndexEntry(self,buzz):
        argsList = []
        argsList.append(str(buzz.uId))
        argsList.append(buzz.user)
        for hashtag in buzz.getHashtags():
            argsList.append(hashtag)
        return ";".join(argsList)

    def updateIndex(self,buzz):
        entry = self.createIndexEntry(buzz)
        filename = INDEX_PATH
        try:
            file = open(filename, "a")
            file.write(entry + '\n')
            file.close()
        except:
            print "Directories badly configured"

    def storeByUser(self,buzz):
        entry = self.createInfoEntry(buzz)
        filename = USERS_PATH + "/" + buzz.user
        try:
            file = open(filename, "a")
            file.write(entry + '\n')
            file.close()
        except:
            print "Directories badly configured"

    def storeByHashtag(self,buzz):
        entry = self.createInfoEntry(buzz)
        hashtags = buzz.getHashtags()
        for hashtag in hashtags:
            filename = HASHTAGS_PATH + "/" + hashtag
            try:
                file = open(filename,"a")
                file.write(entry+'\n')
                file.close()
            except:
                print "Directories badly configured"

    def store(self, buzz):
        self.storeByHashtag(buzz)
        self.storeByUser(buzz)
        self.updateIndex(buzz)


    def getIndexEntry(self, uId):
        filename = INDEX_PATH
        try:
            file = open(filename, 'r+')
            beforePosition = 0
            afterPosition = 0
            for line in file:
                lineValues = line.split(";")
                if(lineValues[0] == uId):
                    afterPosition = beforePosition + len(line)
                    break
                beforePosition += len(line)
            file.seek(beforePosition)
            while(file.tell() < afterPosition-1):
                file.write("-")
            if(afterPosition != 0):
                file.write("\n")
                file.close()
        except Exception as e:
            print e



dbm = DBManager()
buzz = Buzz('luciano','hola #comoestas')
#dbm.store(buzz)
dbm.getIndexEntry('47c74704-3546-4388-9a6a-b857687d1c67')