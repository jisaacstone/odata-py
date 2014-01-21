class RequestParseError(Exception):
    pass

class NotFoundException(Exception):
    pass


class NoContent(Exception):
    def __init__(self, code=204):
        self.code = code
        Exception.__init__(self)
