from QueueManager import QueueManager
from ConnectionManager import ConnectionManager

cm = ConnectionManager()
qm = QueueManager(cm,'testQueue')

qm.writeToQueue('hola mundo!')

cm.close()

