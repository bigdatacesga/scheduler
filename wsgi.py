#!/usr/bin/env python
import logging
import sys
from app import app
from app import mesos

application = app

handler = logging.StreamHandler(sys.stdout)
#handler = logging.FileHandler('./application.log')
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(funcName)s] %(levelname)s: %(message)s '
))
app.logger.addHandler(handler)
# fix gives access to the gunicorn error log facility
app.logger.handlers.extend(logging.getLogger("gunicorn.error").handlers)
# fix gives access to the gunicorn console log facility
app.logger.handlers.extend(logging.getLogger("gunicorn").handlers)

# Initialize a mesos framework instance
master = app.config.get('MESOS_MASTER')
mesos.framework.start(master)

if __name__ == '__main__':
    application.run(threaded=False)
    # When the flask application ends we need to stop also the scheduler thread
    # TODO: Check how to do the same with gunicorn (it does not call __main__)
    mesos.framework.stop()
