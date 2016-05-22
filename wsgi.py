#!/usr/bin/env python
import os
from app import create_app

application = create_app(os.environ.get('FLASK_CONFIG', 'production'))

if __name__ == '__main__':
    application.run(host='127.0.0.1', port=5000)
