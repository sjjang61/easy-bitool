import logging
import logging.handlers

def init_log( filename ):

    # log.basicConfig(filename=filename, level=log.DEBUG)
    log = logging.getLogger()
    log.setLevel(logging.INFO)

    formatter = logging.Formatter('[%(levelname)s] %(asctime)s (%(filename)s:%(lineno)d) %(message)s')
    fileHandler = logging.FileHandler( filename )
    streamHandler = logging.StreamHandler()

    fileHandler.setFormatter(formatter)
    streamHandler.setFormatter(formatter)

    log.addHandler(fileHandler)
    log.addHandler(streamHandler)

def get_logger():
    return logging.getLogger()