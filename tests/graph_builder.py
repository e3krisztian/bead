import datetime
import string
from typing import Dict

from bead.meta import InputSpec
from bead_cli.graph.node import Node
from bead_cli.graph.graph import Ref
from bead_cli.graph.bead_graph import BeadGraph

TS_BASE = datetime.datetime(
    year=2000, month=1, day=1, tzinfo=datetime.timezone.utc
)

DEFAULT_BOX_NAME = 'main'


class GraphBuilder:
    """
    Factory properly connected Nodes.

    For use in test fixtures and to create coherent bead graphs for docs.
    a1 is older than a2, a9 is older than b1
    """
    def __init__(self):
        self._by_name: Dict[str, Node] = {}
        self._phantoms = set()

    def __getitem__(self, name):
        return self._by_name[name]

    def define(self, protos: str, kind: str = '*', box_name: str = DEFAULT_BOX_NAME):
        # 'a1 a2 b3 c4'
        for proto in protos.split():
            bead = self._create(proto, proto, kind, box_name, inputs=[])
            self._by_name[proto] = bead

    def clone(self, proto: str, name: str, box_name: str = DEFAULT_BOX_NAME):
        assert name not in self._by_name
        proto_bead = self._by_name[proto]
        bead = self._create(proto, name, proto_bead.kind, box_name, proto_bead.inputs)
        self._by_name[name] = bead

    def compile(self, dag: str):
        # 'a1 -a-> b2 -> c4 a2 -another-a-> b2'
        label = None
        src: Node | None = None
        for fragment in dag.split():
            if fragment.startswith("-"):
                label = fragment.rstrip(">").strip("-").strip(":")
            else:
                dest = self._by_name[fragment]
                if src is not None and label is not None:
                    self._add_input(dest, label or src.name, src)
                    label = None
                src = dest

    def phantom(self, name_versions: str):
        self._phantoms.update(set(name_versions.split()))

    def ref_for(self, *names):
        for name in names:
            yield Ref.from_bead(self._by_name[name])

    def _create(self, proto, name, kind, box_name, inputs):
        # proto ~ [a-z][0-9]
        proto_name, proto_version = proto
        assert proto_name.islower()
        assert proto_version.isdigit()
        delta_from_name = (
            datetime.timedelta(
                days=string.ascii_lowercase.index(proto_name)))
        delta_from_version = datetime.timedelta(hours=int(proto_version))
        timestamp = TS_BASE + delta_from_name + delta_from_version
        bead = Node(
            name=name.rstrip(string.digits),
            kind=kind,
            content_id=f"content_id_{proto}",
            freeze_time_str=timestamp.strftime('%Y%m%dT%H%M%S%f%z'),
            box_name=box_name,
        )
        # clones share inputs, thus if a new input is added to any of them
        # they still remain clones
        bead.inputs = inputs
        return bead

    def map_input(self, bead_name: str, input_name: str, actual_bead_name: str):
        """Map an input name to the actual bead name via input_map."""
        bead = self._by_name[bead_name]
        bead.input_map[input_name] = actual_bead_name

    def _add_input(self, bead, input_name, input_bead):
        assert input_name not in [i.name for i in bead.inputs]
        input_spec = InputSpec(
            name=input_name,
            kind=input_bead.kind,
            content_id=input_bead.content_id,
            freeze_time_str=input_bead.freeze_time_str,
        )
        bead.inputs.append(input_spec)

    @property
    def beads(self):
        def bead_gen():
            for name, bead in self._by_name.items():
                if name not in self._phantoms:
                    yield bead
        return tuple(bead_gen())

    @property
    def graph(self) -> BeadGraph:
        return BeadGraph.from_beads(self.beads)


def bead(graph: BeadGraph, name_version: str) -> Node:
    for bead in graph.beads:
        if bead.content_id == f'content_id_{name_version}':
            return bead
    raise ValueError('Node by name-version not found', name_version)


if __name__ == '__main__':
    builder = GraphBuilder()
    builder.define('a1 a2', kind='kind1', box_name='secret')
    builder.define('b2', kind='kind2')
    builder.define('c4', kind='kind3')
    builder.define('z9', kind='KK')
    # builder.phantom('a1 a2')
    builder.compile(
        """
        a1 -:older:-> b2 -> c4
        a2 -:newer:-> b2
        """
    )
    builder.clone('b2', 'clone123', 'clone-box')

    from pprint import pprint
    pprint(list(builder.beads))
    pprint([o.__dict__ for o in builder.beads])
