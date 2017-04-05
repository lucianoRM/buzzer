from Buzz import Buzz

ROOT_PATH = './db'
USERS_PATH = './db/users' #stores a file for every user with all buzzes from that user
HASHTAGS_PATH = './db/hashtags' #stores a file for every hashtag with all the buzzes containing that hashtag
INDEX_PATH = './db/index' #stores every saved buzz id and path for deleting

'''Handles db files and stores information'''
class DBManager:


    def __init__(self,keyLength):
        self.keyLength = keyLength

    def createInfoEntry(self,buzz):
        argsList = []
        argsList.append(str(buzz.uId))
        argsList.append(buzz.user)
        argsList.append(buzz.message)
        return ";".join(argsList)



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






