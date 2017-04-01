from QueueManager import QueueManager
from ConnectionManager import ConnectionManager

cm = ConnectionManager()
qm = QueueManager(cm,'luciano')

qm.writeToQueue('stop')

cm.close()

