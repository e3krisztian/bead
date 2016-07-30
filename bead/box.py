'''
We are responsible to store (and retrieve) beads.
'''

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function

from tracelog import TRACELOG  # TODO: remove TRACELOG

from fnmatch import fnmatch
import heapq
from datetime import datetime, timedelta
import functools
from glob import iglob
import os

from .archive import Archive
from . import spec as bead_spec
from .tech.timestamp import time_from_timestamp
from .import tech
Path = tech.fs.Path


# private and specific to Box implementation, when Box gains more power,
# it should change how it handles queries (e.g. using BEAD_NAME_GLOB, KIND,
# or CONTENT_HASH directly through an index)


def _make_checkers():
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
        bead_spec.BEAD_NAME_GLOB: has_name_glob,
        bead_spec.KIND:           has_kind,
        bead_spec.CONTENT_HASH:   has_content_prefix,
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


class _CompareWrapper(object):
    def __init__(self, bead):
        self.bead = bead

    def __eq__(self, other):
        return self.bead.timestamp == other.bead.timestamp


@functools.total_ordering
class _ReverseCompare(_CompareWrapper):
    def __lt__(self, other):
        return self.bead.timestamp > other.bead.timestamp


@functools.total_ordering
class _Compare(_CompareWrapper):
    def __lt__(self, other):
        return self.bead.timestamp < other.bead.timestamp


def order_and_limit_beads(beads, order=bead_spec.NEWEST_FIRST, limit=None):
    '''
    Order beads by timestamps and keep only the closest ones.
    '''
    TRACELOG(beads, order, limit)

    # wrap beads so that they can be compared by timestamps
    compare_wrap = {
        bead_spec.NEWEST_FIRST: _ReverseCompare,
        bead_spec.OLDEST_FIRST: _Compare,
    }[order]
    comparable_beads = (compare_wrap(bead) for bead in beads)

    if limit:
        # assume we have lots of beads, so do it with memory limited
        wrapped_results = heapq.nsmallest(limit, comparable_beads)
        TRACELOG([_.bead.timestamp_str for _ in wrapped_results])
    else:
        wrapped_results = sorted(comparable_beads)

    # unwrap wrapped_results
    return [_.bead for _ in wrapped_results]


ARCHIVE_COMMENT = '''
This file is a BEAD zip archive.

It is a normal zip file that stores a discrete computation of the form

    output = code(*inputs)

The archive contains

- inputs as part of metadata file: references (content hash) to other BEADs
- code   as files
- output as files
- extra metadata to support
  - linking different versions of the same computation
  - determining the newest version
  - reproducing multi-BEAD computation sequences built by a distributed team

There {is,will be,was} more info about BEADs at

- https://unknot.io
- https://github.com/ceumicrodata/bead
- https://github.com/e3krisztian/bead

----

'''


class Box(object):
    # TODO: Box: support user maintained directory hierarchy

    def __init__(self, name=None, location=None):
        self.location = location
        self.name = name

    @property
    def directory(self):
        '''
        Location as a Path.

        Valid only for local boxes.
        '''
        return Path(self.location)

    # XXX: is find_beads in use still?
    def find_beads(self, conditions, order=bead_spec.NEWEST_FIRST, limit=None):
        '''
        Retrieve matching beads.

        (future possibility), it might run in another process,
        potentially on another machine, so it might be faster to restrict
        the results here and not send the whole list over the network.
        '''
        return order_and_limit_beads(self._beads(conditions), order, limit)

    def _beads(self, conditions):
        '''
        Retrieve matching beads.
        '''
        match = compile_conditions(conditions)

        # FUTURE IMPLEMENTATIONS: check for kind & content hash
        # they are good candidates for indexing
        bead_name_globs = [
            value
            for tag, value in conditions
            if tag == bead_spec.BEAD_NAME_GLOB]
        if bead_name_globs:
            glob = bead_name_globs[0] + '*'
        else:
            glob = '*'

        # XXX: directory itself might be a pattern - is it OK?
        paths = iglob(self.directory / glob)
        beads = self._archives_from(paths)
        candidates = (bead for bead in beads if match(bead))
        return candidates

    def _archives_from(self, paths):
        for path in paths:
            try:
                archive = Archive(path, self.name)
            except:
                # bad archive, ignore it
                pass
            else:
                yield archive

    def store(self, workspace, timestamp):
        # -> Bead
        zipfilename = (
            self.directory / (
                '{bead_name}_{timestamp}.zip'
                .format(
                    bead_name=workspace.bead_name,
                    timestamp=timestamp)))
        workspace.pack(zipfilename, timestamp=timestamp, comment=ARCHIVE_COMMENT)
        TRACELOG('store as archive', zipfilename)
        return Archive(zipfilename)

    def find_names(self, kind, content_hash, timestamp):
        '''
        -> (exact_match, best_guess, best_guess_timestamp, names)
        where
            exact_match          = name (kind & content_hash matched)
            best_guess           = name (kind matched, timestamp is closest to input's)
            best_guess_timestamp = timestamp ()
            names                = sequence of names (kind matched)
        '''
        assert isinstance(timestamp, datetime)
        paths = (self.directory / fname for fname in os.listdir(self.directory))
        beads = self._archives_from(paths)
        candidates = (bead for bead in beads if bead.kind == kind)

        exact_match          = None
        best_guess           = None
        best_guess_timestamp = None
        best_guess_timedelta = None
        names                = set()
        for bead in candidates:
            if bead.content_hash == content_hash:
                exact_match = bead.name
            #
            bead_timestamp = time_from_timestamp(bead.timestamp_str)
            bead_timedelta = bead_timestamp - timestamp
            if bead_timedelta < timedelta():
                bead_timedelta = -bead_timedelta
            if (
                (best_guess_timedelta is None) or
                (bead_timedelta < best_guess_timedelta) or
                (bead_timedelta == best_guess_timedelta and bead_timestamp > best_guess_timestamp)
            ):
                best_guess = bead.name
                best_guess_timestamp = bead_timestamp
                best_guess_timedelta = bead_timedelta
            #
            names.add(bead.name)

        return exact_match, best_guess, best_guess_timestamp, names

    def get_context(self, string_type, string, time):
        # in theory timestamps can be [intentionally] duplicated, but let's
        # treat that as an error condition to be fixed ASAP
        conditions = [(string_type, string)]
        return make_context(time, self._beads(conditions))


class UnionBox:
    def __init__(self, boxes):
        self.boxes = tuple(boxes)

    def get_context(self, string_type, string, time):
        context = None
        for box in self.boxes:
            try:
                box_context = box.get_context(string_type, string, time)
            except LookupError:
                continue
            else:
                context = merge_contexts(box_context, context)

        if context:
            return context
        raise LookupError

    def get_at(self, string_type, string, time):
        context = self.get_context(string_type, string, time)
        return context.best


class BeadContext:
    def __init__(self, time, bead, prev, next):
        assert bead is None or bead.timestamp == time
        assert prev is None or prev.timestamp < time
        assert next is None or next.timestamp > time
        assert bead or prev or next
        self.time = time
        self.bead = bead
        self.prev = prev
        self.next = next

    @property
    def best(self):
        if self.bead:
            return self.bead
        if not self.prev:
            return self.next
        if not self.next:
            return self.prev
        if self.time - self.prev.timestamp < self.next.timestamp - self.time:
            return self.prev
        return self.next


def make_context(time, beads):
    match, prev, next = None, None, None
    for bead in beads:
        if bead.timestamp < time:
            if prev is None or prev.timestamp < bead.timestamp:
                prev = bead
        elif bead.timestamp > time:
            if next is None or bead.timestamp < next.timestamp:
                next = bead
        else:
            assert bead.timestamp == time
            assert match is None or match.content_hash == bead.content_hash, (
                'multiple beads with same timestamp')
            match = bead
    if match or prev or next:
        return BeadContext(time, match, prev, next)
    raise LookupError


def merge_contexts(context1, context2):
    if context1 is None:
        return context2
    if context2 is None:
        return context1
    assert context1.time == context2.time
    time = context1.time
    beads = (
        context1.bead, context1.prev, context1.next,
        context2.bead, context2.prev, context2.next)
    beads = (bead for bead in beads if bead)
    return make_context(time, beads)
