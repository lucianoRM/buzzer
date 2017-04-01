from User import User
import time

user = User('luciano')

user.login()
time.sleep(3)
print "logout"
user.logout()



