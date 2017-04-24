import ConfigParser

CONFIG_FILE_NAME="config/config.ini"

Config = ConfigParser.ConfigParser()
Config.read(CONFIG_FILE_NAME)



def ip():
    return Config.get(section='Shared',option="IP")

def port():
    return int(Config.get(section='Shared',option="PORT"))

def timeout():
    return int(Config.get(section='Shared',option="TIMEOUT"))

def dbExchange():
    return Config.get(section='DBExchange',option="EXCHANGE_NAME")

def indexExchange():
    return Config.get(section='IndexExchange',option="EXCHANGE_NAME")

def dbIndexManagerFilePath():
    return Config.get(section='DBIndexManager',option="INDEX_PATH")

def dbBuzzManagerFilePath():
    return Config.get(section='DBBuzzManager',option="ROOT_PATH")

def indexKeyLength():
    return int(Config.get(section='IndexPool',option="FILE_KEY_LENGTH"))


def buzzKeyLength():
    return int(Config.get(section='BuzzPool',option="FILE_KEY_LENGTH"))


def poolSize():
    return int(Config.get(section='Pool', option="SIZE"))

def dispatcherQueue():
    return Config.get(section='Dispatcher', option="QUEUE_NAME")


def ttQueueName():
    return Config.get(section='TTManager', option="QUEUE_NAME")

def ttCountKey():
    return Config.get(section='TTManager', option="COUNT_KEY")

def ttLastMessagesKey():
    return Config.get(section='TTManager', option="LAST_MESSAGES_KEY")

def ttTotalTTs():
    return int(Config.get(section='TTManager', option='TOTAL_TTS'))

def ttMaxMSGS():
    return int(Config.get(section='TTManager', option="MAX_MSGS"))

def registrationQueueName():
    return Config.get(section='UserRegistrationHandler', option="QUEUE_NAME")

def registrationFolder():
    return Config.get(section='UserRegistrationHandler', option="REGISTRATION_FOLDER")

def registrationUsersInfoFolder():
    return Config.get(section='UserRegistrationHandler', option="USERS_INFO_FOLDER")








