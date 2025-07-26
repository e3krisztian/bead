'''
We are responsible to store (and retrieve) beads.

We are a convenience feature, as beads can be stored and used directly as files.
It is assumed, that boxes store disjunct sets of data.
This implies, that when beads are branched (a copy is made), the copy process should
- rename all the beads and update their input-maps
- copy the beads to a new box, that will never be active at the same time, as the original

Boxes can be used to:
- share computations (beads) (when the box is on a shared drive (e.g. NFS or sshfs mounted))
- store separate computation branches (e.g. versions, that are released to the public)
- hide sensitive computations by splitting up storage according to access level
  (this is naive access control, but could work)
'''

from datetime import datetime, timedelta
import os
from typing import Iterator, Iterable, Sequence

from .ziparchive import ZipArchive
from .bead import Archive
from .exceptions import InvalidArchive
from .exceptions import BoxError
from . import spec as bead_spec
from .tech.timestamp import time_from_timestamp
from .import tech
Path = tech.fs.Path


# private and specific to Box implementation, when Box gains more power,
# it should change how it handles queries (e.g. using BEAD_NAME, KIND,
# or CONTENT_ID directly through an index)


def _make_checkers():
    def has_name(name):
        def filter(bead):
            return bead.name == name
        return filter

    def has_kind(kind):
        def filter(bead):
            return bead.kind == kind
        return filter

    def has_content_prefix(prefix):
        def filter(bead):
            return bead.content_id.startswith(prefix)
        return filter

    return {
        bead_spec.BEAD_NAME:  has_name,
        bead_spec.KIND:       has_kind,
        bead_spec.CONTENT_ID: has_content_prefix,
    }


_CHECKERS = _make_checkers()


def compile_conditions(conditions):
    '''
    Compile list of (check-type, check-param)-s into a match function.
    '''
    checkers = [_CHECKERS[check_type](check_param) for check_type, check_param in conditions]

    def match(bead):
        for check in checkers:
            if not check(bead):
                return False
        return True
    return match


ARCHIVE_COMMENT = '''
This file is a BEAD zip archive.

It is a normal zip file that stores a discrete computation of the form

    output = code(*inputs)

The archive contains

- inputs as part of metadata file: references (content_id) to other BEADs
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


class Box:
    """
    Store Beads.
    """

    def __init__(self, name: str, location: Path):
        self.location = location
        self.name = name

    @property
    def directory(self):
        '''
        Location as a Path.

        Valid only for local boxes.
        '''
        return Path(self.location)

    def find_bead(self, name, content_id):
        query = ((bead_spec.BEAD_NAME, name), (bead_spec.CONTENT_ID, content_id))
        for bead in self._beads(query):
            return bead

    def find_bead_by_content_id(self, content_id):
        """Find bead by exact content_id match."""
        query = ((bead_spec.CONTENT_ID, content_id),)
        for bead in self._beads(query):
            if bead.content_id == content_id:
                return bead
        return None

    def all_beads(self) -> Iterator[Archive]:
        '''
        Iterator for all beads in this Box
        '''
        return iter(self._beads([]))

    def _beads(self, conditions) -> Iterable[Archive]:
        '''
        Retrieve matching beads.
        '''
        match = compile_conditions(conditions)

        bead_names = set(
            value
            for tag, value in conditions
            if tag == bead_spec.BEAD_NAME)
        if bead_names:
            if len(bead_names) > 1:
                # easy path: names disagree
                return []
            # beadname_20170615T075813302092+0200.zip
            glob = bead_names.pop() + '_????????T????????????[-+]????.zip'
        else:
            glob = '*'

        paths = self.directory.glob(glob)
        beads = self._archives_from(paths)
        candidates = (bead for bead in beads if match(bead))
        return candidates

    def _archives_from(self, paths: Iterable[Path]):
        for path in paths:
            try:
                archive = ZipArchive(path, self.name)
            except InvalidArchive:
                # TODO: log/report problem
                pass
            else:
                yield archive

    def store(self, workspace, freeze_time):
        # -> Bead
        if not self.directory.exists():
            raise BoxError(f'Box "{self.name}": directory {self.directory} does not exist')
        if not self.directory.is_dir():
            raise BoxError(f'Box "{self.name}": {self.directory} is not a directory')
        zipfilename = (
            self.directory / f'{workspace.name}_{freeze_time}.zip')
        workspace.pack(zipfilename, freeze_time=freeze_time, comment=ARCHIVE_COMMENT)
        return zipfilename

    def find_names(self, kind, content_id, timestamp):
        '''
        -> (exact_match, best_guess, best_guess_freeze_time, names)
        where
            exact_match            = name (kind & content_id matched)
            best_guess             = name (kind matched, timestamp is closest to input's)
            best_guess_freeze_time = timestamp ()
            names                  = sequence of names (kind matched)
        '''
        assert isinstance(timestamp, datetime)
        try:
            filenames = os.listdir(self.directory)
        except FileNotFoundError:
            filenames = []
        paths = (self.directory / fname for fname in filenames)
        beads = self._archives_from(paths)
        candidates = (bead for bead in beads if bead.kind == kind)

        exact_match            = None
        best_guess             = None
        best_guess_freeze_time = None
        best_guess_timedelta   = None
        names                  = set()
        for bead in candidates:
            if bead.content_id == content_id:
                exact_match = bead.name
            #
            bead_freeze_time = time_from_timestamp(bead.freeze_time_str)
            bead_timedelta = bead_freeze_time - timestamp
            if bead_timedelta < timedelta():
                bead_timedelta = -bead_timedelta
            if (
                (best_guess_timedelta is None) or
                (bead_timedelta < best_guess_timedelta) or
                (
                    (bead_timedelta == best_guess_timedelta) and
                    (bead_freeze_time > best_guess_freeze_time)
                )
            ):
                best_guess = bead.name
                best_guess_freeze_time = bead_freeze_time
                best_guess_timedelta = bead_timedelta
            #
            names.add(bead.name)

        return exact_match, best_guess, best_guess_freeze_time, names

    def get_context(self, check_type, check_param, time):
        # in theory timestamps can be [intentionally] duplicated, but let's
        # treat that as an error condition to be fixed ASAP
        conditions = [(check_type, check_param)]
        return make_context(time, self._beads(conditions))


class UnionBox:
    def __init__(self, boxes: Sequence[Box]):
        self.boxes = tuple(boxes)

    def get_context(self, check_type, check_param, time):
        context = None
        for box in self.boxes:
            try:
                box_context = box.get_context(check_type, check_param, time)
            except LookupError:
                continue
            else:
                context = merge_contexts(box_context, context)

        if context:
            return context
        raise LookupError

    def get_at(self, check_type, check_param, time):
        context = self.get_context(check_type, check_param, time)
        return context.best

    def all_beads(self) -> Iterator[Archive]:
        '''
        Iterator for all beads in this Box
        '''
        for box in self.boxes:
            yield from box.all_beads()


class BeadContext:
    def __init__(self, time, bead, prev, next):
        assert bead is None or bead.freeze_time == time
        assert prev is None or prev.freeze_time < time
        assert next is None or next.freeze_time > time
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
        if self.time - self.prev.freeze_time < self.next.freeze_time - self.time:
            return self.prev
        return self.next


def make_context(time, beads):
    match, prev, next = None, None, None
    for bead in beads:
        if bead.freeze_time < time:
            if prev is None or prev.freeze_time < bead.freeze_time:
                prev = bead
        elif bead.freeze_time > time:
            if next is None or bead.freeze_time < next.freeze_time:
                next = bead
        else:
            assert bead.freeze_time == time
            assert match is None or match.content_id == bead.content_id, (
                'multiple beads with same freeze time')
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
