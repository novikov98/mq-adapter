class RabbitException(Exception):
    def __init__(self, message=""):
        super().__init__()
        self.message = message


class EnvelopeHandlerNotRegisteredError(RabbitException):
    pass
