#!/usr/bin/env python
import sys
from app import app
import logging

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

if __name__ == '__main__':
    application.run()
