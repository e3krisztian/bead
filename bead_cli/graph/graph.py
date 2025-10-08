from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Sequence

from .node import Node
from .node import Ref


@dataclass(frozen=True)
class Edge:
    src: Node
    dest: Node
    label: str = ''

    def reversed(self):
        return Edge(self.dest, self.src, self.label)

    @cached_property
    def src_ref(self):
        return self.src.ref

    @cached_property
    def dest_ref(self):
        return self.dest.ref


def generate_input_edges(node_index: dict[Ref, Node], bead: Node) -> Iterator[Edge]:
    """
    Generate all the 'Edge's leading from the bead to its inputs.

    Modifies node_index - adds referenced, but missing beads as phantom nodes.

    An edge is a triple of (src, dest, label), where both 'src' and 'dest' are Nodes.
    """
    for input in bead.inputs:
        src_ref = Ref.from_bead(input)
        try:
            src = node_index[src_ref]
        except LookupError:
            src = node_index[src_ref] = Node.phantom_from_input(bead, input)

        yield Edge(src, bead, input.name)


def group_by_src(edges) -> dict[Ref, list[Edge]]:
    """
    Make a dictionary of 'Edge's, which maps a src node to a list of 'Edge's rooted there.
    """
    edges_by_src: dict[Ref, list[Edge]] = defaultdict(list)
    for edge in edges:
        edges_by_src[edge.src_ref].append(edge)
    return edges_by_src


def group_by_dest(edges) -> dict[Ref, list[Edge]]:
    """
    Make a dictionary of 'Edge's, which maps a node to a list of 'Edge's going there.
    """
    edges_by_dest: dict[Ref, list[Edge]] = defaultdict(list)
    for edge in edges:
        edges_by_dest[edge.dest_ref].append(edge)
    return edges_by_dest


def closure(roots: list[Ref], edges_by_src: dict[Ref, list[Edge]]) -> set[Ref]:
    """
    Return the set of reachable nodes from roots.
    edges_by_src is edges grouped by their `src`.
    """
    reachable: set[Ref] = set()
    todo: set[Ref] = set(roots)
    while todo:
        src = todo.pop()
        reachable.add(src)
        for edge in edges_by_src[src]:
            dest_id = edge.dest_ref
            if dest_id not in reachable:
                todo.add(dest_id)
    return reachable


def reverse(edges: Iterable[Edge]) -> Iterator[Edge]:
    """
    Generate reversed edges.
    """
    return (edge.reversed() for edge in edges)


def node_index_from_edges(edges: Iterable[Edge]) -> dict[Ref, Node]:
    node_by_ref: dict[Ref, Node] = {}

    def register_map(ref, node):
        value = node_by_ref.setdefault(ref, node)
        assert node == value
    for e in edges:
        register_map(e.src_ref, e.src)
        register_map(e.dest_ref, e.dest)

    return node_by_ref


def refs_from_nodes(nodes: Iterable[Node]) -> set[Ref]:
    return {node.ref for node in nodes}


bead_index_from_edges = node_index_from_edges
refs_from_beads = refs_from_nodes


def refs_from_edges(edges: Iterable[Edge]) -> set[Ref]:
    return set(node_index_from_edges(edges))


def toposort(edges: Sequence[Edge]) -> list[Node]:
    """
    Topological sort.
    """
    edges_by_dest = group_by_dest(edges)
    node_by_ref = node_index_from_edges(edges)

    todo = set(node_by_ref.keys())
    path: list[Ref] = []
    output: list[Node] = []

    def dfs(ref: Ref):
        if ref in path:
            raise ValueError('Loop detected!', path, ref)

        path.append(ref)
        for input_edge in edges_by_dest[ref]:
            input_node = input_edge.src
            if input_node.ref in todo:
                dfs(input_node.ref)
        path.pop()

        todo.remove(ref)
        output.append(node_by_ref[ref])

    while todo:
        dfs(next(iter(todo)))

    return output
