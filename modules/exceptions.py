class ConnectorParseError(BaseException):
    def __init__(self, msg=None):
        self.msg = msg

class ConnectorHttpError(BaseException):
    def __init__(self, msg=None):
        self.msg = msg
