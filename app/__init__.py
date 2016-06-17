import os
from flask import Flask
from flask import Blueprint
#from . import mesos

app = Flask(__name__)
config_name = os.environ.get('FLASK_CONFIG', 'development')
cfg = os.path.join(os.getcwd(), 'config', config_name + '.py')
app.config.from_pyfile(cfg)

# Create a blueprint
api = Blueprint('api', __name__)
# Import the endpoints belonging to this blueprint
from . import endpoints
from . import errors

# register blueprints
app.register_blueprint(api, url_prefix='/bigdata/mesos_framework/v1')

# Initialize a mesos framework instance
#master = app.config.get('MESOS_MASTER')
#mesos.framework.start(master)
