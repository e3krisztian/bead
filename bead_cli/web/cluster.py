from typing import Dict
from typing import Iterable

from bead.tech.timestamp import EPOCH_STR

from . import graphviz
from .dummy import Dummy
from .freshness import Freshness


class Cluster:
    """
    Versions of beads having the same name.

    .head: Latest bead, that is not phantom, or the first phantom bead, if all are phantoms.
    """
    def __init__(self, name):
        self.name = name
        self.beads_by_content_id = {}

        # use a phantom bead instead of None for default value
        phantom_head = (
            Dummy(
                name=name,
                freeze_time_str=EPOCH_STR,
                content_id='PhantomBead',
                kind='EMPTY CLUSTER'))
        phantom_head.set_freshness(Freshness.PHANTOM)
        self.head = phantom_head

    def add(self, bead):
        assert bead.name == self.name
        assert bead.content_id not in self.beads_by_content_id
        self.beads_by_content_id[bead.content_id] = bead

        def head_order(bead):
            return (bead.is_not_phantom, bead.freeze_time)

        if head_order(bead) >= head_order(self.head):
            self.head = bead

    def beads(self):
        """
        Time sorted list of beads, most recent first.
        """
        return (
            sorted(
                self.beads_by_content_id.values(),
                key=(lambda bead: bead.freeze_time),
                reverse=True))

    def reset_freshness(self):
        beads = self.beads()

        if beads and beads[0].is_not_phantom:
            beads[0].set_freshness(Freshness.UP_TO_DATE)

        for bead in beads[1:]:
            bead.set_freshness(Freshness.SUPERSEDED)

    @property
    def as_dot(self):
        return ''.join(graphviz.dot_cluster_as_fragments(self.name, self.beads()))

    def __len__(self):
        return len(self.beads_by_content_id)


def create_cluster_index(beads: Iterable[Dummy]) -> Dict[str, Cluster]:
    cluster_by_name: Dict[str, Cluster] = {}
    for bead in beads:
        if bead.name not in cluster_by_name:
            cluster_by_name[bead.name] = Cluster(bead.name)
        cluster_by_name[bead.name].add(bead)
    return cluster_by_name
