from db.DBBuzzProcessingPool import DBBuzzProcessingPool
from listener.Dispatcher import Dispatcher
from user.User import User
from utils.ThreadSafeVariable import ThreadSafeVariable

v = ThreadSafeVariable(True)
d1 = DBBuzzProcessingPool(["a.#"])
d1.start(v)
d2 = DBBuzzProcessingPool(["b.#","c.#"])
d2.start(v)

dis = Dispatcher()
dis.start(v)

u = User('luciano')

for i in xrange(100):
    raw_input()
    u.sendBuzz("hola")

