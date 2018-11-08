
import logging
import logging.config

def log_config( log_name, logging_config=None, default_level=logging.INFO ):
    if logging_config:
        logging.config.dictConfig(logging_config)
    else:
        logging.basicConfig(level=default_level)
    global logger
    logger = logging.getLogger(log_name)

def log_info(*msg):
    msg_str = ' '.join(msg)
    logger.info(msg_str)

def log_error(*msg):
    msg_str = ' '.join(msg)
    logger.error(msg_str)

def log_exception(*msg):
    msg_str = ' '.join(msg)
    logger.exception(msg_str)

