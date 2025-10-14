ENV       = 'directory that defines e.g. the boxes'
WORKSPACE = 'workspace directory'
BEAD      = '''which bead to use - supports mini-language syntax:

Syntax: [[box:]name][@time] | file_path

Basic forms:
  - bead name: "hotel-dataset"
  - box + name: "archive:hotel-dataset"
  - file path: "/path/to/bead.zip"

Time constraints:
  - partial timestamp: "hotel-dataset@2024-06-15"
  - year/month: "hotel-dataset@2024" or "hotel-dataset@2024-06"
  - with box: "archive:hotel-dataset@2024-06-15"

Relative versions:
  - previous: "hotel-dataset@-" (or @--, @---, @----)
  - next: "hotel-dataset@+" (or @++, @+++, @++++)
  - explicit count: "hotel-dataset@-5" or "hotel-dataset@+10"

Context-based (for input commands):
  - box only: "archive:" (uses input name)
  - time only: "@2024-06-15" (uses input name)
  - combined: "archive:@-" (uses input name, from archive box, previous version)
'''
INPUT_NAME = (
    'name of input,'
    + ' its workspace relative location is "input/%(metavar)s"')
BOX = 'Name of box to store bead'
