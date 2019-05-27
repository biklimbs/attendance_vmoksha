import logging
import logging.config
import sys
sys.path.append('/usr/local/lib/python3.5/dist-packages')
import coloredlogs
import os
DIR="./"
LOG_DIR = "log"
LOG_FILE = "log.txt"


def configure_logger(name, log_path):
    logging.config.dictConfig({
        'version': 1,
        'formatters': {
            'default': {'format': '%(asctime)s %(levelname)s %(filename)s %(lineno)s %(funcName)s %(message)s', 'datefmt': '%Y-%m-%d %H:%M:%S'}
        },
        'handlers': {
            'console': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'default',
                'stream': 'ext://sys.stdout'
            },
            'file': {
                'level': 'INFO',
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'default',
                'filename': log_path,
                'maxBytes': 2000000,
                'backupCount': 5
            }
        },
        'loggers': {
            'default': {
                'level': 'DEBUG',
                'handlers': ['console', 'file']
            }
        },
        'disable_existing_loggers': False
    })
    return logging.getLogger(name)
#coloredlogs.install()
if not os.path.isdir(""+DIR+""+LOG_DIR+""):
		os.system('mkdir '+LOG_DIR+'')

#log = configure_logger('default', ""+DIR+""+LOG_DIR+"/"+LOG_FILE+"")
