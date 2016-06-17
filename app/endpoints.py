from flask import jsonify, request
from . import app, api
from mesos import framework
import registry


@api.route('/clusters', methods=['POST'])
def submit_cluster():
    """Submit a new cluster instance to Mesos"""
    # Only application/json requests are valid
    if is_valid(request):
        data = request.get_json()
        clusterdn = data['clusterdn']
        app.logger.info('POST /clusters: {}'.format(clusterdn))
        cluster = registry.Cluster(clusterdn)
        framework.submit(cluster)
        return jsonify({'message': 'Service instance queued'}), 200
    else:
        app.logger.warn('POST /clusters: Invalid request')
        return jsonify({'status': '400',
                        'error': 'Invalid request',
                        'message': 'Unable to get the clusterdn'}), 400


@api.route('/clusters/<clusterdn>', methods=['DELETE'])
def kill_cluster(clusterdn):
    """Kill a cluster instance"""
    cluster = registry.Cluster(clusterdn)
    framework.kill(cluster)
    return '', 204


@api.route('/clusters', methods=['GET'])
def list_clusters():
    """Get a list of cluster instances"""
    return jsonify({"queued_tasks": framework.pending()})


def is_valid(request):
    """Validate a cluster submission request"""
    return request.get_json() and 'clusterdn' in request.get_json()
