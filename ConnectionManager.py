import pika

QUEUE_IP = 'localhost'
QUEUE_PORT = 5672

class ConnectionManager:

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(QUEUE_IP, QUEUE_PORT))
        self.channel = self.connection.channel()

    def close(self):
        self.connection.close()


