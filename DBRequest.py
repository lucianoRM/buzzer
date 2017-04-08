



class DBRequest:

    def __init__(self,user):
        self.user = user

class QueryRequest(DBRequest):

    def __init__(self,user,tag):
        DBRequest.__init__(self,user)
        self.tag = tag

class DeleteRequest(DBRequest):

    def __init__(self,user,uId):
        DBRequest.__init__(self,user)
        self.uId = uId