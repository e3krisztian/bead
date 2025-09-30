'''
layout of beads
'''

from . import tech

Path = tech.fs.Path


class Archive:

    META = 'meta'
    CODE = 'code'
    DATA = 'data'

    BEAD_META = f'{META}/bead'
    MANIFEST = f'{META}/manifest'

    # volatile content, not included in generation of content_id
    INPUT_MAP = f'{META}/input.map'


class Workspace:

    INPUT = Path('input')
    OUTPUT = Path('output')
    TEMP = Path('temp')
    META = Path('.bead-meta')

    BEAD_META = META / 'bead'
    INPUT_MAP = META / 'input.map'
