from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function

from fnmatch import fnmatch


# QUERY_ORDER:
NEWEST_FIRST = 'newest_first'
OLDEST_FIRST = 'oldest_first'

# QUERY_WHERE:
OLDER_THAN     = 'OLDER_THAN'
NEWER_THAN     = 'NEWER_THAN'
BEAD_NAME_GLOB = 'BEAD_NAME_GLOB'
KIND           = 'KIND'
CONTENT_HASH   = 'CONTENT_HASH'
# TODO: support shortened content hashes
NEXT_VERSION   = 'NEXT_VERSION'
PREV_VERSION   = 'PREV_VERSION'


# private and specific to Box implementation, when Box gains
# more power, it should change how it handles queries (e.g. using KIND
# or CONTENT_HASH directly through an index)

def _make_checkers():
    def is_newer_than(timestamp):
        def filter(bead):
            return bead.timestamp > timestamp
        return filter

    def is_older_than(timestamp):
        def filter(bead):
            return bead.timestamp < timestamp
        return filter

    def has_name_glob(nameglob):
        def filter(bead):
            return fnmatch(bead.name, nameglob)
        return filter

    def has_kind(kind):
        def filter(bead):
            return bead.kind == kind
        return filter

    def has_content_prefix(hash_prefix):
        def filter(bead):
            return bead.content_hash.startswith(hash_prefix)
        return filter

    return {
        OLDER_THAN:     is_older_than,
        NEWER_THAN:     is_newer_than,
        BEAD_NAME_GLOB: has_name_glob,
        KIND:           has_kind,
        CONTENT_HASH:   has_content_prefix,
    }

_CHECKERS = _make_checkers()


def compile_conditions(conditions):
    '''
    Compile list of (check-name, check-param)-s into a match function.
    '''
    checkers = [_CHECKERS[check](param) for check, param in conditions]

    def match(bead):
        for check in checkers:
            if not check(bead):
                return False
        return True
    return match
