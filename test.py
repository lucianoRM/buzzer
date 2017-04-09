from DBBuzzProcessingPool import DBBuzzProcessingPool
from Dispatcher import Dispatcher
from ThreadSafeVariable import ThreadSafeVariable
from User import User

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

