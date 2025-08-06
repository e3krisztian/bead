import datetime
import string
from typing import Dict, Optional

from bead.meta import InputSpec
from bead_cli.web.dummy import Dummy
from bead_cli.web.graph import Ref
from bead_cli.web.sketch import Sketch


TS_BASE = datetime.datetime(
    year=2000, month=1, day=1, tzinfo=datetime.timezone.utc
)

DEFAULT_BOX_NAME = 'main'


class Sketcher:
    """
    Factory properly connected Dummy-s.

    For use in test fixtures and to create coherent bead web graphs for docs.
    a1 is older than a2, a9 is older than b1
    """
    def __init__(self):
        self._by_name: Dict[str, Dummy] = {}
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
        src: Optional[Dummy] = None
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
        bead = Dummy(
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
    def sketch(self) -> Sketch:
        return Sketch.from_beads(self.beads)


def bead(sketch: Sketch, name_version: str) -> Dummy:
    for bead in sketch.beads:
        if bead.content_id == f'content_id_{name_version}':
            return bead
    raise ValueError('Dummy by name-version not found', name_version)


if __name__ == '__main__':
    sketcher = Sketcher()
    sketcher.define('a1 a2', kind='kind1', box_name='secret')
    sketcher.define('b2', kind='kind2')
    sketcher.define('c4', kind='kind3')
    sketcher.define('z9', kind='KK')
    # sketcher.phantom('a1 a2')
    sketcher.compile(
        """
        a1 -:older:-> b2 -> c4
        a2 -:newer:-> b2
        """
    )
    sketcher.clone('b2', 'clone123', 'clone-box')

    from pprint import pprint
    pprint(list(sketcher.beads))
    pprint([o.__dict__ for o in sketcher.beads])
