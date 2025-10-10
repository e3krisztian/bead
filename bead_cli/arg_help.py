ENV       = 'directory that defines e.g. the boxes'
WORKSPACE = 'workspace directory'
BEAD      = '''
    which bead to use
    - a bead name (e.g., "hotel-dataset")
    - an archive file path (e.g., "/path/to/bead.zip")
'''
INPUT_NAME = (
    'name of input,'
    + ' its workspace relative location is "input/%(metavar)s"')
BOX = 'Name of box to store bead'
