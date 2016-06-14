from flask import jsonify, request
from . import api, framework


@api.route('/instance/', methods=['POST'])
@api.route('/instance', methods=['POST'])
def launch_new_instance():
    # Handle Content-Type: application/json requests
    if request.get_json():
        data = request.get_json()
        instance_path = data['instance_dn']
        framework.add_task_to_queue(instance_path)
        return jsonify({'status': '200',
                        'message': 'Service instance queued'}), 200
    # Handle form param requests: eg. curl -d status=free
    else:
        return jsonify({'status': '400',
                        'error': 'Invalid request',
                        'message': 'Unable to get the instance path'}), 400


@api.route('/instances/', methods=['GET'])
@api.route('/instances', methods=['GET'])
def get_instances():
    return jsonify({"queued_tasks": framework.get_queued_instances()})
