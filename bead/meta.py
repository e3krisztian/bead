'''
Keys for accessing fields in .BEAD_META structures

.BEAD_META files are persistent python dictionaries,
with the following minimum structure:

{
    meta_version: ...,
    inputs: {
        'nick1' : {
            kind: ...,
            content_hash: ...,
            freeze_time: ...,
        },
        'nick2' : {
            kind: ...,
            content_hash: ...,
            freeze_time: ...,
        },
        ...
    },
    kind: ...,
    freeze_time: ...,  # only archives - naive ordering
    freeze_name: ...,  # only archives, bead name for bootstrapping
}
'''

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function


from collections import namedtuple
from .tech.timestamp import time_from_timestamp

# Metadata versions determine the content-hash used and potentially
# other processing differences. Having it in the metadata potentially
# enables interoperability and backwards compatibility of BEADs.
# If the content-hash ever needs to be changed/upgraded we still
# want existing BEADs to remain connected and alive.

META_VERSION = 'meta_version'

KIND = 'kind'

INPUTS = 'inputs'

INPUT_KIND    = 'kind'
INPUT_CONTENT_HASH = 'content_hash'
INPUT_FREEZE_TIME  = 'freeze_time'


class InputSpec(namedtuple('InputSpec', 'name kind content_hash timestamp_str')):
    @property
    def timestamp(self):
        return time_from_timestamp(self.timestamp_str)


def parse_inputs(meta):
    '''
    Parse and yield input specification from meta as records.
    '''
    inputs = meta[INPUTS]
    for name in inputs:
        spec = inputs[name]
        yield InputSpec(
            name,
            spec[INPUT_KIND],
            spec[INPUT_CONTENT_HASH],
            spec[INPUT_FREEZE_TIME])


# Archive meta:
FREEZE_TIME = 'freeze_time'
FREEZE_NAME = 'freeze_name'
