class InvalidArchive(Exception):
    """Not a valid bead archive"""


class BoxError(Exception):
    """Box operation related error"""


class BoxIndexError(Exception):
    """Box index operation related error"""
    def __init__(self, message, *, advice=None):
        super().__init__(message)
        self.advice = advice

