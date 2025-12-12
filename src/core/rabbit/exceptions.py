class RabbitException(Exception):
    def __init__(self, message: str = ""):
        super().__init__(message)
        self.message = message


class EnvelopeHandlerNotRegisteredError(RabbitException):
    pass


class DuplicateEnvelopeError(RabbitException):
    pass


class EnvelopeProcessingError(RabbitException):
    def __init__(self, message: str = "", requeue: bool = False):
        super().__init__(message)
        self.requeue = requeue
