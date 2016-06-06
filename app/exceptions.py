class ValidationError(ValueError):
    pass

class ResourceException(Exception):
    def __init__(self, message):
        super(ResourceException, self).__init__(message)

