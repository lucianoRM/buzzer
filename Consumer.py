from QueueManager import QueueManager
from ConnectionManager import ConnectionManager

cm = ConnectionManager()
qm = QueueManager(cm,'testQueue')

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)


qm.listenToQueue(callback)

cm.close()