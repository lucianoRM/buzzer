import fcntl

from DBManager import DBManager



HASHTAG_INDEX_PATH = './index/hashtags'

'''Saves the index to locate users and hashtags in messages'''
class DBIndexManager:

    def __init__(self,keyLength):
        self.keyLength = keyLength

    def storeHashtagsIndex(self,buzz):
        hashtags = buzz.getHashtags()
        for hashtag in hashtags:
            self.storeHashtagIndex(hashtag,buzz.uId)

    def getHashtagFileKey(self,hashtag):
        if (len(hashtag) < self.keyLength):
            fileKey = hashtag
        else:
            fileKey = hashtag[:self.keyLength]
        return fileKey

    def getTagPosition(self,filelines,tag):
        position = 0
        for line in filelines:
            args = line.split(';')
            if(args[0] == tag):
                return position
            position+=1
        return -1

    def getFileLines(self,filename):
        file = open(filename,'r')
        fcntl.flock(file,fcntl.LOCK_SH)
        lines = file.readlines()
        fcntl.flock(file,fcntl.LOCK_UN)
        file.close()
        return lines

    def writeLines(self,filename,lines):
        file = open(filename, 'w')
        fcntl.flock(file, fcntl.LOCK_EX)
        file.writelines(lines)
        fcntl.flock(file, fcntl.LOCK_UN)
        file.close()

    def storeHashtagIndex(self,hashtag,uId):
        filename = HASHTAG_INDEX_PATH + "/" + self.getHashtagFileKey(hashtag)
        try:
            filelines = self.getFileLines(filename)
        except IOError as e:
            print e
        position = self.getTagPosition(filelines, hashtag)
        if(position >= 0):
            filelines[position] = filelines[position] + ";" + uId
        else:
            filelines.append(hashtag + ";" + uId)
        self.writeLines(filename,filelines)


m = DBIndexManager(5)

m.storeHashtagIndex('#luciano','1')
m.storeHashtagIndex('#sosoaos','2')
m.storeHashtagIndex('#lucas','3')
m.storeHashtagIndex('#luciano','7')
m.storeHashtagIndex('#','80')


