import pika
import random
import string

from Buzz import Buzz
from DBRequest import DBRequest, QueryRequest, DeleteRequest
from MessageUtils import MessageUtils

names = ['luciano','juan','pedro','pepe','roberto']

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='buzz-exchange',
                         type='topic')
query = raw_input("ahora: ")
args = query.split(" ")
if(args[0] == 'q'):
    r = QueryRequest(args[1],args[2])
elif(args[0] == 'b'):
    print 'sent buzz'
    r = Buzz(args[1],args[2])
elif(args[0] == 'd'):
    r = DeleteRequest(args[1],args[2])
elif(args[0] == 'run-loop'):
    for i in xrange(100):
        r = Buzz(random.choice(names),''.join(random.choice(string.ascii_uppercase) for _ in range(5)))
        channel.basic_publish(exchange='buzz-exchange',routing_key='asdgadg',body=MessageUtils.serialize(r))
    exit(0)

channel.basic_publish(exchange='buzz-exchange',
                      routing_key='asdgadg',
                      body=MessageUtils.serialize(r))


connection.close()
