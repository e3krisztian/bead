from typing import Iterable, Dict, List, TypeVar

import attr
from cached_property import cached_property

from bead.meta import InputSpec, InputName, BeadName
from bead.tech.timestamp import time_from_timestamp
from .freshness import Freshness


InputMap = Dict[InputName, BeadName]


def input_map_converter(value) -> InputMap:
    """attr converter"""
    if value is None:
        return {}
    return {InputName(k): BeadName(v) for k, v in value.items()}


@attr.s(auto_attribs=True)
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
    input_map: InputMap = attr.ib(kw_only=True, factory=dict, converter=input_map_converter)
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
            input_map=bead.input_map,
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
                name=bead.get_input_bead_name(inputspec.name),
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

    def get_input_bead_name(self, input_nick):
        '''
        Returns the bead name on which update works.
        '''
        return self.input_map.get(input_nick, input_nick)

    def set_input_bead_name(self, input_nick, bead_name):
        '''
        Sets the bead name to be used for updates in the future.
        '''
        self.input_map[input_nick] = bead_name

    def __repr__(self):
        cls = self.__class__.__name__
        kind = self.kind[:8]
        content_id = self.content_id[:8]
        inputs = repr(self.inputs)
        return f"{cls}:{self.name}:{kind}:{content_id}:{self.freshness}:{inputs}:{self.input_map}"


Bead = TypeVar('Bead')


@attr.s(frozen=True, slots=True, auto_attribs=True)
class Ref:
    """
    Unique reference for Dummy-es.

    NOTE: Using multiple boxes can make this reference non-unique, as it is possible to
    have beads with same name and content, but with different input-maps.
    This could happen e.g. for sets of beads that are "branched" - released - and
    possibly need separate future maintenance.

    This potential non-unique-ness need to be explicitly mitigated.
    E.g. by refusing to work if there is an ambiguity in the repos
    (after reporting which packages have disagreeing instances and where).
    """
    name: str
    content_id: str

    @classmethod
    def from_bead(cls, bead: Dummy) -> 'Ref':
        return cls(bead.name, bead.content_id)

    @classmethod
    def from_bead_input(cls, bead: Dummy, input: InputSpec) -> 'Ref':
        src_bead_name = bead.get_input_bead_name(input.name)
        return cls(src_bead_name, input.content_id)

    @classmethod
    def index_for(cls, beads: Iterable[Dummy]) -> Dict['Ref', Dummy]:
        bead_by_ref = {}
        for bead in beads:
            bead_by_ref[cls.from_bead(bead)] = bead
        return bead_by_ref
