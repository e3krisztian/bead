from typing import Dict
from typing import Iterable
from typing import List
from typing import TypeVar

import attr
from cached_property import cached_property

from bead.meta import InputSpec
from bead.tech.timestamp import time_from_timestamp

from .freshness import Freshness


@attr.s(auto_attribs=True, repr=False, str=False)
class Dummy:
    """
    A bead.Bead look-alike when looking only at the metadata.

    Also has metadata for coloring (freshness).
    """
    # these are considered immutable once the object is created
    name: str = attr.ib(kw_only=True, default="UNKNOWN")
    content_id: str = attr.ib(kw_only=True)
    kind: str = attr.ib(kw_only=True)
    freeze_time_str: str = attr.ib(kw_only=True)
    inputs: List[InputSpec] = attr.ib(kw_only=True, factory=list, converter=list)

    # these can be modified after the object is created
    freshness: Freshness = attr.ib(kw_only=True, default=Freshness.SUPERSEDED, converter=Freshness)
    box_name: str = attr.ib(kw_only=True, default='')

    @cached_property
    def freeze_time(self):
        return time_from_timestamp(self.freeze_time_str)

    @cached_property
    def ref(self) -> 'Ref':
        return Ref.from_bead(self)

    @classmethod
    def from_bead(cls, bead):
        return cls(
            name=bead.name,
            content_id=bead.content_id,
            kind=bead.kind,
            freeze_time_str=bead.freeze_time_str,
            inputs=bead.inputs,
            freshness=getattr(bead, 'freshness', Freshness.SUPERSEDED),
            box_name=bead.box_name)

    @classmethod
    def phantom_from_input(cls, bead: 'Dummy', inputspec: InputSpec):
        """
        Create phantom beads from inputs.

        The returned bead is referenced as input from another bead,
        but we do not have the referenced bead.
        """
        phantom = (
            cls(
                name=inputspec.name,
                content_id=inputspec.content_id,
                kind=inputspec.kind,
                freeze_time_str=inputspec.freeze_time_str))
        phantom.freshness = Freshness.PHANTOM
        return phantom

    def set_freshness(self, freshness):
        # phantom beads do not change freshness
        if self.freshness != Freshness.PHANTOM:
            self.freshness = freshness

    @property
    def is_not_phantom(self):
        return self.freshness != Freshness.PHANTOM

    def __repr__(self):
        cls = self.__class__.__name__
        kind = self.kind[:8]
        content_id = self.content_id[:8]
        inputs = repr(self.inputs)
        return f"{cls}:{self.name}:{kind}:{content_id}:{self.freshness}:{inputs}"


Bead = TypeVar('Bead')


@attr.s(frozen=True, slots=True, auto_attribs=True)
class Ref:
    """
    Unique reference for Dummy-s.

    NOTE: Using multiple boxes can make resolution of references non-unique, as it is possible to
    have beads with same name and content.
    """
    name: str
    content_id: str

    @classmethod
    def from_bead(cls, bead: Dummy | InputSpec) -> 'Ref':
        return cls(bead.name, bead.content_id)

    @classmethod
    def index_for(cls, beads: Iterable[Dummy]) -> Dict['Ref', Dummy]:
        bead_by_ref = {}
        for bead in beads:
            bead_by_ref[cls.from_bead(bead)] = bead
        return bead_by_ref
