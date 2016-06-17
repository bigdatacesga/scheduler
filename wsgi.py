#!/usr/bin/env python
import logging
import sys
from app import app as application
from app import mesos


def setup_flask_logging():
    # Log to stdout
    #handler = logging.StreamHandler(sys.stdout)
    # Log to a file
    handler = logging.FileHandler('./application.log')
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(
        '%(asctime)s [%(funcName)s] %(levelname)s: %(message)s '
    ))
    application.logger.addHandler(handler)


def setup_gunicorn_logging():
    # fix gives access to the gunicorn error log facility
    application.logger.handlers.extend(logging.getLogger("gunicorn.error").handlers)
    # fix gives access to the gunicorn console log facility
    application.logger.handlers.extend(logging.getLogger("gunicorn").handlers)


# Set default log level for the general logger
# each handler can then restrict the messages logged
application.logger.setLevel(logging.INFO)
setup_flask_logging()
#setup_gunicorn_logging()


# Initialize a mesos framework instance
master = application.config.get('MESOS_MASTER')
mesos.framework.start(master)

if __name__ == '__main__':
    #import pdb; pdb.set_trace()
    application.run(threaded=False)
    # When the flask application ends we need to stop also the scheduler thread
    # TODO: Check how to do the same with gunicorn (it does not call __main__)
    mesos.framework.stop()
