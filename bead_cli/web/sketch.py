import itertools
from typing import Set, Dict, List, Tuple, Sequence, Iterable

import attr
from cached_property import cached_property

from bead.tech.timestamp import EPOCH_STR
from .freshness import UP_TO_DATE, OUT_OF_DATE
from .dummy import Dummy
from .cluster import Cluster, create_cluster_index
from .io import read_beads, write_beads
from . import graphviz
from .graph import (
    Edge,
    Ref,
    generate_input_edges,
    group_by_src,
    group_by_dest,
    toposort,
    closure,
    bead_index_from_edges,
    refs_from_beads,
    refs_from_edges,
)


@attr.s(frozen=True, auto_attribs=True)
class Sketch:
    beads: Tuple[Dummy, ...]
    edges: Tuple[Edge, ...]

    def __attrs_post_init__(self):
        assert refs_from_edges(self.edges) - refs_from_beads(self.beads) == set()

    @classmethod
    def from_beads(cls, beads: Sequence[Dummy]):
        bead_index = Ref.index_for(beads)
        edges = tuple(
            itertools.chain.from_iterable(
                generate_input_edges(bead_index, bead)
                for bead in beads))
        return cls(tuple(bead_index.values()), edges)

    @classmethod
    def from_edges(cls, edges: Sequence[Edge]):
        beads = bead_index_from_edges(edges).values()
        return cls(tuple(beads), tuple(edges))

    @classmethod
    def from_file(cls, file_name):
        beads = read_beads(file_name)
        return cls.from_beads(beads)

    def to_file(self, file_name):
        write_beads(file_name, self.beads)

    @cached_property
    def cluster_by_name(self) -> Dict[str, Cluster]:
        return create_cluster_index(self.beads)

    @cached_property
    def clusters(self):
        return tuple(self.cluster_by_name.values())

    def color_beads(self):
        color_beads(self)

    def as_dot(self):
        return plot_clusters_as_dot(self)

    def drop_deleted_inputs(self) -> "Sketch":
        return drop_deleted_inputs(self)


def simplify(sketch: Sketch) -> Sketch:
    """
    Remove unreferenced clusters and beads.

    Makes a new instance
    """
    raise NotImplementedError


def heads_of(sketch: Sketch) -> Sketch:
    """
    Keep only cluster heads and their inputs.

    Makes a new instance
    """
    head_by_ref = {c.head.ref: c.head for c in sketch.clusters}
    head_edges = tuple(e for e in sketch.edges if e.dest_ref in head_by_ref)
    src_by_ref = {e.src_ref: e.src for e in head_edges}
    heads = {**head_by_ref, **src_by_ref}.values()
    return Sketch(beads=tuple(heads), edges=head_edges)


def add_final_sink_to(sketch: Sketch) -> Tuple[Sketch, Dummy]:
    """
    Add a new node, and edges from all nodes.

    This makes a DAG fully connected and the new node a sink node.
    The added sink node is special (guaranteed to have a unique name, freshness is UP_TO_DATE).
    Returns the extended Sketch and the new sink node.

    Makes a new instance
    """
    sink_name = '*' * (1 + max((len(bead.name) for bead in sketch.beads), default=0))
    sink = Dummy(
        name=sink_name,
        content_id=sink_name,
        kind=sink_name,
        freeze_time_str='SINK',
        freshness=UP_TO_DATE
    )
    sink_edges = (Edge(src, sink) for src in sketch.beads)
    return (
        Sketch(
            beads=sketch.beads + tuple([sink]),
            edges=sketch.edges + tuple(sink_edges)
        ),
        sink
    )


def set_sources(sketch: Sketch, cluster_names: Iterable[str]) -> Sketch:
    """
    Drop all clusters, that are not reachable from the named clusters.

    Makes a new instance
    """
    cluster_filter = ClusterFilter(sketch)

    edges = cluster_filter.get_encoded_edges()
    root_refs = cluster_filter.get_encoded_refs(cluster_names)

    cluster_refs_to_keep = closure(root_refs, group_by_src(edges))

    return cluster_filter.get_filtered_by_refs(cluster_refs_to_keep)


def set_sinks(sketch: Sketch, cluster_names: Iterable[str]) -> Sketch:
    """
    Drop all clusters, that do not lead to any of the named clusters.

    Makes a new instance
    """
    cluster_filter = ClusterFilter(sketch)

    edges = cluster_filter.get_encoded_edges()
    edges = [e.reversed() for e in edges]
    root_refs = cluster_filter.get_encoded_refs(cluster_names)

    cluster_refs_to_keep = closure(root_refs, group_by_src(edges))

    return cluster_filter.get_filtered_by_refs(cluster_refs_to_keep)


class ClusterFilter:
    def __init__(self, sketch):
        self.sketch = sketch
        self.dummy_by_name = {
            name: Dummy(
                name=name,
                content_id=name,
                kind=name,
                freeze_time_str=EPOCH_STR,
            )
            for name in sketch.cluster_by_name
        }

    def get_encoded_edges(self) -> Sequence[Edge]:
        src_dest_pairs = self.convert_to_name_pairs(self.sketch.edges)
        return [
            Edge(
                self.dummy_by_name[src],
                self.dummy_by_name[dest])
            for src, dest in src_dest_pairs
        ]

    def get_encoded_refs(self, bead_names: Iterable[str]) -> List[Ref]:
        return [
            self.dummy_by_name[name].ref
            for name in sorted(set(bead_names))
            if name in self.dummy_by_name
        ]

    def get_filtered_by_refs(self, encoded_refs) -> Sketch:
        src_dest_pairs = self.convert_to_name_pairs(self.sketch.edges)
        clusters_to_keep = {r.name for r in encoded_refs}
        cluster_edges_to_keep = {
            (src, dest)
            for src, dest in src_dest_pairs
            if src in clusters_to_keep and dest in clusters_to_keep
        }
        encoded_edges = [
            Edge(
                self.dummy_by_name[src],
                self.dummy_by_name[dest],
            )
            for src, dest in cluster_edges_to_keep
        ]
        return self.get_filtered_by_edges(encoded_edges)

    def get_filtered_by_edges(self, encoded_edges: Iterable[Edge]) -> Sketch:
        src_dest_pairs = self.convert_to_name_pairs(encoded_edges)
        bead_names = {src for src, _ in src_dest_pairs} | {dest for _, dest in src_dest_pairs}
        assert bead_names - set(self.dummy_by_name) == set()
        beads = tuple(b for b in self.sketch.beads if b.name in bead_names)
        edges = tuple(e for e in self.sketch.edges if (e.src.name, e.dest.name) in src_dest_pairs)
        return Sketch(beads, edges).drop_deleted_inputs()

    def convert_to_name_pairs(self, edges: Iterable[Edge]) -> Set[Tuple[str, str]]:
        return {(e.src.name, e.dest.name) for e in edges}


def drop_before(sketch: Sketch, timestamp) -> Sketch:
    """
    Keep only beads, that are after the given timestamp.

    Makes a new instance
    """
    raise NotImplementedError


def drop_after(sketch: Sketch, timestamp) -> Sketch:
    """
    Keep only beads, that are before the timestamp.

    Makes a new instance
    """
    raise NotImplementedError


def plot_clusters_as_dot(sketch: Sketch):
    """
    Generate GraphViz .dot file content, which describe the connections between beads
    and their up-to-date status.
    """
    formatted_bead_clusters = '\n\n'.join(c.as_dot for c in sketch.clusters)
    graphviz_context = graphviz.Context()

    def format_inputs():
        def edges_as_dot():
            for edge in sketch.edges:
                is_auxiliary_edge = (
                    edge.dest.freshness not in (OUT_OF_DATE, UP_TO_DATE))

                yield graphviz_context.dot_edge(edge.src, edge.dest, edge.label, is_auxiliary_edge)
        return '\n'.join(edges_as_dot())

    return graphviz.DOT_GRAPH_TEMPLATE.format(
        bead_clusters=formatted_bead_clusters,
        bead_inputs=format_inputs())


def color_beads(sketch: Sketch) -> bool:
    """
    Assign up-to-dateness status (freshness) to beads.
    """
    heads, sink = add_final_sink_to(heads_of(sketch))
    head_eval_order = toposort(heads.edges)
    if not head_eval_order:  # empty
        return True
    assert head_eval_order[-1] == sink

    for cluster in sketch.clusters:
        cluster.reset_freshness()

    # downgrade UP_TO_DATE freshness if has a non UP_TO_DATE input
    edges_by_dest = group_by_dest(heads.edges)
    for cluster_head in head_eval_order:
        if cluster_head.freshness is UP_TO_DATE:
            if any(e.src.freshness is not UP_TO_DATE for e in edges_by_dest[cluster_head.ref]):
                cluster_head.set_freshness(OUT_OF_DATE)

    return sink.freshness is UP_TO_DATE


def drop_deleted_inputs(sketch: Sketch) -> Sketch:
    edges_as_refs = {(edge.src_ref, edge.dest_ref) for edge in sketch.edges}
    beads = []
    for bead in sketch.beads:
        inputs_to_keep = []
        for input in bead.inputs:
            input_ref = Ref.from_bead(input)
            if (input_ref, bead.ref) in edges_as_refs:
                inputs_to_keep.append(input)
        beads.append(attr.evolve(bead, inputs=inputs_to_keep))
    return Sketch.from_beads(beads)
