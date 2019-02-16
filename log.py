import logging
import logging.handlers

from gevent import lock

BASIC_LOG_FORMAT = "%(asctime)s:[%(levelname)s]:[%(process)s]:[%(name)s]:%(message)s"
TIME_FORMAT = "%d/%b/%Y:%H:%M:%S"

'''
Class: Logger,  meant to return logger for each file
'''

LOGGING_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}


class Logger:
    _lock = lock.RLock()

    @staticmethod
    def get_logger(name='root'):
        n = name.split('embark')[-1].split('.')[0].replace('/', '.').strip('.')
        level = logging.DEBUG
        logger = logging.getLogger(n)
        logger.setLevel(level)
        formatter = logging.Formatter(BASIC_LOG_FORMAT, TIME_FORMAT)
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        if len(logger.handlers) == 0:
            with Logger._lock:
                if len(logger.handlers) == 0:
                    logger.addHandler(handler)
        logger.propagate = False
        return logger


if __name__ == "__main__":
    Logger.get_logger(__file__)