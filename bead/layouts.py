'''
Layout constants for bead archives and workspaces.

Workspace → Archive mapping when packing:
  workspace root files/dirs  →  archive code/
  workspace output/          →  archive data/
  workspace .bead-meta/      →  archive meta/
  workspace input/           →  (not archived)
  workspace temp/            →  (not archived)
'''

from .infra.fs import Path


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
