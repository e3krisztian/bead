import itertools
from dataclasses import dataclass
from dataclasses import replace as dataclass_replace
from functools import cached_property
from collections.abc import Iterable
from collections.abc import Sequence

from bead.infra.timestamp import EPOCH_ISO

from . import graphviz
from .cluster import Cluster
from .cluster import create_cluster_index
from .node import Node
from .freshness import OUT_OF_DATE
from .freshness import UP_TO_DATE
from .graph import Edge
from .graph import Ref
from .graph import bead_index_from_edges
from .graph import closure
from .graph import generate_input_edges
from .graph import group_by_dest
from .graph import group_by_src
from .graph import refs_from_beads
from .graph import refs_from_edges
from .graph import toposort
from .io import read_beads
from .io import write_beads


@dataclass(frozen=True)
class BeadGraph:
    beads: tuple[Node, ...]
    edges: tuple[Edge, ...]

    def __post_init__(self):
        assert refs_from_edges(self.edges) - refs_from_beads(self.beads) == set()

    @classmethod
    def from_beads(cls, beads: Sequence[Node]):
        node_index = Ref.index_for(beads)
        # Use list() to snapshot beads before generate_input_edges modifies node_index
        deduplicated_beads = list(node_index.values())
        edges = tuple(
            itertools.chain.from_iterable(
                generate_input_edges(node_index, bead)
                for bead in deduplicated_beads))
        # node_index may now contain phantom nodes added during edge generation
        return cls(tuple(node_index.values()), edges)

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
    def cluster_by_name(self) -> dict[str, Cluster]:
        return create_cluster_index(self.beads)

    @cached_property
    def clusters(self):
        return tuple(self.cluster_by_name.values())

    def color_beads(self) -> bool:
        """
        Assign up-to-dateness status (freshness) to beads.
        """
        heads, sink = add_final_sink_to(heads_of(self))
        head_eval_order = toposort(heads.edges)
        if not head_eval_order:  # empty
            return True
        assert head_eval_order[-1] == sink

        for cluster in self.clusters:
            cluster.reset_freshness()

        # downgrade UP_TO_DATE freshness if has a non UP_TO_DATE input
        edges_by_dest = group_by_dest(heads.edges)
        for cluster_head in head_eval_order:
            if cluster_head.freshness is UP_TO_DATE:
                if any(e.src.freshness is not UP_TO_DATE for e in edges_by_dest[cluster_head.ref]):
                    cluster_head.set_freshness(OUT_OF_DATE)

        return sink.freshness is UP_TO_DATE

    def as_dot(self):
        """
        Generate GraphViz .dot file content, which describe the connections between beads
        and their up-to-date status.
        """
        formatted_bead_clusters = '\n\n'.join(c.as_dot for c in self.clusters)
        graphviz_context = graphviz.Context()

        def format_inputs():
            def edges_as_dot():
                for edge in self.edges:
                    is_auxiliary_edge = (
                        edge.dest.freshness not in (OUT_OF_DATE, UP_TO_DATE))

                    yield graphviz_context.dot_edge(edge.src, edge.dest, edge.label, is_auxiliary_edge)
            return '\n'.join(edges_as_dot())

        return graphviz.DOT_GRAPH_TEMPLATE.format(
            bead_clusters=formatted_bead_clusters,
            bead_inputs=format_inputs())

    def drop_deleted_inputs(self) -> "BeadGraph":
        edges_as_refs = {(edge.src_ref, edge.dest_ref) for edge in self.edges}
        beads = []
        for bead in self.beads:
            inputs_to_keep = []
            for input in bead.inputs:
                input_ref = Ref.from_bead(input)
                if (input_ref, bead.ref) in edges_as_refs:
                    inputs_to_keep.append(input)
            beads.append(dataclass_replace(bead, inputs=inputs_to_keep))
        return BeadGraph.from_beads(beads)


def simplify(graph: BeadGraph) -> BeadGraph:
    """
    Remove unreferenced clusters and beads.

    Makes a new instance
    """
    raise NotImplementedError


def heads_of(graph: BeadGraph) -> BeadGraph:
    """
    Keep only cluster heads and their inputs.

    Makes a new instance
    """
    head_by_ref = {c.head.ref: c.head for c in graph.clusters}
    head_edges = tuple(e for e in graph.edges if e.dest_ref in head_by_ref)
    src_by_ref = {e.src_ref: e.src for e in head_edges}
    heads = {**head_by_ref, **src_by_ref}.values()
    return BeadGraph(beads=tuple(heads), edges=head_edges)


def add_final_sink_to(graph: BeadGraph) -> tuple[BeadGraph, Node]:
    """
    Add a new node, and edges from all nodes.

    This makes a DAG fully connected and the new node a sink node.
    The added sink node is special (guaranteed to have a unique name, freshness is UP_TO_DATE).
    Returns the extended BeadGraph and the new sink node.

    Makes a new instance
    """
    sink_name = '*' * (1 + max((len(bead.name) for bead in graph.beads), default=0))
    sink = Node(
        name=sink_name,
        content_id=sink_name,
        kind=sink_name,
        freeze_time_iso='SINK',
        freshness=UP_TO_DATE
    )
    sink_edges = (Edge(src, sink) for src in graph.beads)
    return (
        BeadGraph(
            beads=graph.beads + tuple([sink]),
            edges=graph.edges + tuple(sink_edges)
        ),
        sink
    )


def set_sources(graph: BeadGraph, cluster_names: Iterable[str]) -> BeadGraph:
    """
    Drop all clusters, that are not reachable from the named clusters.

    Makes a new instance
    """
    cluster_filter = ClusterFilter(graph)

    edges = cluster_filter.get_encoded_edges()
    root_refs = cluster_filter.get_encoded_refs(cluster_names)

    cluster_refs_to_keep = closure(root_refs, group_by_src(edges))

    return cluster_filter.get_filtered_by_refs(cluster_refs_to_keep)


def set_sinks(graph: BeadGraph, cluster_names: Iterable[str]) -> BeadGraph:
    """
    Drop all clusters, that do not lead to any of the named clusters.

    Makes a new instance
    """
    cluster_filter = ClusterFilter(graph)

    edges = cluster_filter.get_encoded_edges()
    edges = [e.reversed() for e in edges]
    root_refs = cluster_filter.get_encoded_refs(cluster_names)

    cluster_refs_to_keep = closure(root_refs, group_by_src(edges))

    return cluster_filter.get_filtered_by_refs(cluster_refs_to_keep)


class ClusterFilter:
    def __init__(self, graph):
        self.graph = graph
        self.node_by_name = {
            name: Node(
                name=name,
                content_id=name,
                kind=name,
                freeze_time_iso=EPOCH_ISO,
            )
            for name in graph.cluster_by_name
        }

    def get_encoded_edges(self) -> Sequence[Edge]:
        src_dest_pairs = self.convert_to_name_pairs(self.graph.edges)
        return [
            Edge(
                self.node_by_name[src],
                self.node_by_name[dest])
            for src, dest in src_dest_pairs
        ]

    def get_encoded_refs(self, bead_names: Iterable[str]) -> list[Ref]:
        return [
            self.node_by_name[name].ref
            for name in sorted(set(bead_names))
            if name in self.node_by_name
        ]

    def get_filtered_by_refs(self, encoded_refs) -> BeadGraph:
        src_dest_pairs = self.convert_to_name_pairs(self.graph.edges)
        clusters_to_keep = {r.name for r in encoded_refs}
        cluster_edges_to_keep = {
            (src, dest)
            for src, dest in src_dest_pairs
            if src in clusters_to_keep and dest in clusters_to_keep
        }
        encoded_edges = [
            Edge(
                self.node_by_name[src],
                self.node_by_name[dest],
            )
            for src, dest in cluster_edges_to_keep
        ]
        return self.get_filtered_by_edges(encoded_edges)

    def get_filtered_by_edges(self, encoded_edges: Iterable[Edge]) -> BeadGraph:
        src_dest_pairs = self.convert_to_name_pairs(encoded_edges)
        bead_names = {src for src, _ in src_dest_pairs} | {dest for _, dest in src_dest_pairs}
        assert bead_names - set(self.node_by_name) == set()
        beads = tuple(b for b in self.graph.beads if b.name in bead_names)
        edges = tuple(e for e in self.graph.edges if (e.src.name, e.dest.name) in src_dest_pairs)
        return BeadGraph(beads, edges).drop_deleted_inputs()

    def convert_to_name_pairs(self, edges: Iterable[Edge]) -> set[tuple[str, str]]:
        return {(e.src.name, e.dest.name) for e in edges}


def drop_before(graph: BeadGraph, timestamp) -> BeadGraph:
    """
    Keep only beads, that are after the given timestamp.

    Makes a new instance
    """
    raise NotImplementedError


def drop_after(graph: BeadGraph, timestamp) -> BeadGraph:
    """
    Keep only beads, that are before the timestamp.

    Makes a new instance
    """
    raise NotImplementedError






