import logging
from logging.config import dictConfig

dictConfig({
    'version': 1,
    'formatters': {
        'standard': {
            'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'INFO'
        },
        'file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'standard',
            'filename': 'center-control-dock.log',
            'when': 'D',
            'interval': 1,
            'backupCount': 7,
            'encoding': 'utf8'
        }
    },
    'loggers': {
        'default': {
            'level': 'INFO',
            'handlers': ['file'],
            'propagate': False
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['file']
    }
})

log = logging.getLogger('default')