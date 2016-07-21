'''
We are responsible to store (and retrieve) beads.
'''

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function

from tracelog import TRACELOG  # TODO: remove TRACELOG

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


class _Wrapper(object):
    def __init__(self, wrapped):
        self.wrapped = wrapped

    def __eq__(self, other):
        return self.wrapped.timestamp == other.wrapped.timestamp


@functools.total_ordering
class _MoreIsLess(_Wrapper):
    def __lt__(self, other):
        return self.wrapped.timestamp > other.wrapped.timestamp


@functools.total_ordering
class _LessIsLess(_Wrapper):
    def __lt__(self, other):
        return self.wrapped.timestamp < other.wrapped.timestamp


def order_and_limit_beads(beads, order=bead_spec.NEWEST_FIRST, limit=None):
    '''
    Order beads by timestamps and keep only the closest ones.
    '''
    TRACELOG(beads, order, limit)

    # wrap beads so that they can be compared by timestamps
    compare_wrap = {
        bead_spec.NEWEST_FIRST: _MoreIsLess,
        bead_spec.OLDEST_FIRST: _LessIsLess,
    }[order]
    comparable_beads = (compare_wrap(bead) for bead in beads)

    if limit:
        # assume we have lots of beads, so do it with memory limited
        wrapped_results = heapq.nsmallest(limit, comparable_beads)
        TRACELOG([w.wrapped.timestamp_str for w in wrapped_results])
    else:
        wrapped_results = sorted(comparable_beads)

    # unwrap wrapped_results
    return [wrapper.wrapped for wrapper in wrapped_results]


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

    # FIXME: find_beads(self, time) -> (right before, exactly at, right after)
    # i.e. a triple of beads
    # in theory timestamps can be [intentionally] duplicated, but let's
    # treat that as an error condition to be fixed ASAP

    def find_beads(self, conditions, order=bead_spec.NEWEST_FIRST, limit=None):
        '''
        Retrieve matching beads.

        (future possibility), it might run in another process,
        potentially on another machine, so it might be faster to restrict
        the results here and not send the whole list over the network.
        '''
        match = bead_spec.compile_conditions(conditions)

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

        return order_and_limit_beads(candidates, order, limit)

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
