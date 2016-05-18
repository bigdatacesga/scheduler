from flask import jsonify
from werkzeug.exceptions import BadRequest
from kvstore import KeyDoesNotExist
from .exceptions import ValidationError
from . import api


@api.errorhandler(ValidationError)
def validation_error(error):
    response = jsonify({'status': 400, 'error': 'validation error',
                        'message': error.message})
    response.status_code = 400
    return response


@api.errorhandler(BadRequest)
def bad_request(error):
    response = jsonify({'status': 400, 'error': 'bad request',
                        'message': error.message})
    response.status_code = 400
    return response


@api.errorhandler(KeyDoesNotExist)
def key_does_not_exist(error):
    response = jsonify({'status': 400, 'error': 'Unable to find it',
                        'message': error.message})
    response.status_code = 400
    return response


@api.errorhandler(KeyError)
def key_error(error):
    response = jsonify({'status': 400, 'error': 'Unable to find it',
                        'message': error.message})
    response.status_code = 400
    return response


@api.app_errorhandler(404)  # this has to be an app-wide handler
def not_found(error):
    response = jsonify({'status': 404, 'error': 'not found',
                        'message': 'invalid resource URI'})
    response.status_code = 404
    return response


@api.errorhandler(405)
def method_not_supported(error):
    response = jsonify({'status': 405, 'error': 'method not supported',
                        'message': 'the method is not supported'})
    response.status_code = 405
    return response


@api.app_errorhandler(500)  # this has to be an app-wide handler
def internal_server_error(error):
    response = jsonify({'status': 500, 'error': 'internal server error',
                        'message': error.message})
    response.status_code = 500
    return response
