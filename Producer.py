from Buzz import Buzz
from QueueManager import QueueManager
from ConnectionManager import ConnectionManager

cm = ConnectionManager()
qm = QueueManager(cm,'luciano')

buzz = Buzz("mensaje", "producer")

qm.writeToQueue("sadgasa")

cm.close()

