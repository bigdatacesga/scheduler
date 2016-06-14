import os
from flask import Flask
from flask import Blueprint
from mesos_framework.framework import MesosFramework

# Initialize a mesos framework instance
framework = MesosFramework("")

# Create a blueprint
api = Blueprint('api', __name__)
# Import the endpoints belonging to this blueprint
from . import endpoints
from . import errors


def create_app(config_name):
    """Create an application instance."""
    app = Flask(__name__)

    # apply configuration
    cfg = os.path.join(os.getcwd(), 'config', config_name + '.py')
    app.config.from_pyfile(cfg)

    # register blueprints
    app.register_blueprint(api, url_prefix='/bigdata/mesos_framework/v1')

    return app
