class InvalidArchive(Exception):
    """Not a valid bead archive"""


class BoxError(Exception):
    """Box operation related error"""


class BoxIndexError(Exception):
    """Box index operation related error"""
    def __init__(self, message, *, advice=None, box_name=None, index_path=None):
        super().__init__(message)
        self.advice = advice
        self.box_name = box_name
        self.index_path = index_path

    @property
    def formatted_advice(self):
        """Get advice with command placeholders substituted."""
        if not self.advice:
            return self.advice

        if self.box_name:
            return self.advice.replace('{REINDEX_COMMAND}', f'bead box reindex {self.box_name}')
        else:
            return self.advice.replace('{REINDEX_COMMAND}', 'bead box reindex')

