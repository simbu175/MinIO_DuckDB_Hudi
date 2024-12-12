import os
import logging
import logging.handlers
import configparser
from config import base_config as bc

LEVELS = {f'DEBUG': logging.DEBUG,
          f'INFO': logging.INFO,
          f'WARNING': logging.WARNING,
          f'ERROR': logging.ERROR,
          f'CRITICAL': logging.CRITICAL
          }

LOGCONFIGFILE = bc.BASE_PATH + f"/config/default_logger.config"
LOG_PATH = bc.BASE_PATH + f"/logs"


# LOGCONFIGFILE = f"D:\\Initial_Version\\Lake_Ingestion_Codes\\config\\logger.config"


def getconfig(section=f'DEFAULT'):
    config = configparser.ConfigParser()
    config.read(LOGCONFIGFILE)
    return (config.get(section, f'logfile'),
            config.get(section, f'defaultlevel'),
            config.get(section, f'logmode'),
            )


def basiclogger(logname, section=f'DEFAULT', format_type=f"basic"):
    logfile, deflevel, logmode = getconfig(section)
    # This will help us to log messages in different log files at a time.
    logger = logging.getLogger(logname + '_' + section)
    logger.setLevel(LEVELS[deflevel])
    mode = {f'append': f'a', f'overwrite': f'w'}[logmode]
    logfile = os.path.join(LOG_PATH, logfile)
    if not logger.handlers:
        handler = logging.handlers.RotatingFileHandler(filename=logfile,
                                                       mode=mode,
                                                       maxBytes=0,
                                                       backupCount=0
                                                       )

        if format_type == f"extended":
            formatter = logging.Formatter(
                f"%(asctime)s :: %(levelname)s :: %(name)s ::  %(module)s  "
                f":: %(funcName)s :: %(lineno)s :: %(message)s")
        else:
            formatter = logging.Formatter(f'%(asctime)s :: %(levelname)s '
                                          f':: %(name)s :: %(message)s')

        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


if __name__ == f"__main__":
    print(f"Custom Logging Module")
    blogger = basiclogger(logname=f'default')
    blogger.info(f'This is a debug message')
