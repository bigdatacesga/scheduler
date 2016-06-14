from flask import jsonify, request
from . import api
from mesos_framework.framework import MesosFramework

framework = MesosFramework()

@api.route('/instance', methods=['POST'])
@api.route('/instance', methods=['DELETE'])
def handle_instance_call():
    # Handle Content-Type: application/json requests
    if request.get_json():
        data = request.get_json()
        instance_path = data['instance_dn']
        try:
            if request.method == 'POST':
                framework.add_task_to_queue(instance_path)
            elif request.method == 'DELETE':
                framework.kill_instance(instance_path)
            else:
                raise Exception("Invalid call method")
        except Exception as e:
            return jsonify({'status': '400',
                            'error': 'Invalid instance data',
                            'message': e.message}), 400

        return jsonify({'status': '200',
                        'message': 'Service instance destroyed'}), 200
    # Handle form param requests: eg. curl -d status=free
    else:
        return jsonify({'status': '400',
                        'error': 'Invalid request',
                        'message': 'Unable to get the instance path'}), 400

@api.route('/instances', methods=['GET'])
def get_instances():
    return jsonify({"queued_tasks": framework.get_queued_instances()})



