import pika

class ConnectionManager:

    def __init__(self,ip,port):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(ip, port))
        self.channel = self.connection.channel()

    def close(self):
        self.connection.close()


