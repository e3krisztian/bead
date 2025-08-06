from collections import defaultdict
from typing import Iterable, Dict, List, Set, Iterator, Sequence

import attr
from cached_property import cached_property

from .dummy import Dummy, Ref


Node = Dummy


@attr.s(auto_attribs=True, frozen=True)
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


def generate_input_edges(bead_index: Dict[Ref, Dummy], bead: Dummy) -> Iterator[Edge]:
    """
    Generate all the 'Edge's leading from the bead to its inputs.

    Modifies bead_index - adds referenced, but missing beads as phantom beads.

    An edge is a triple of (src, dest, label), where both 'src' and 'dest' are Dummy-s.
    """
    for input in bead.inputs:
        src_ref = Ref.from_bead(input)
        try:
            src = bead_index[src_ref]
        except LookupError:
            src = bead_index[src_ref] = Dummy.phantom_from_input(bead, input)

        yield Edge(src, bead, input.name)


def group_by_src(edges) -> Dict[Ref, List[Edge]]:
    """
    Make a dictionary of 'Edge's, which maps a src node to a list of 'Edge's rooted there.
    """
    edges_by_src: Dict[Ref, List[Edge]] = defaultdict(list)
    for edge in edges:
        edges_by_src[edge.src_ref].append(edge)
    return edges_by_src


def group_by_dest(edges) -> Dict[Ref, List[Edge]]:
    """
    Make a dictionary of 'Edge's, which maps a node to a list of 'Edge's going there.
    """
    edges_by_dest: Dict[Ref, List[Edge]] = defaultdict(list)
    for edge in edges:
        edges_by_dest[edge.dest_ref].append(edge)
    return edges_by_dest


def closure(roots: List[Ref], edges_by_src: Dict[Ref, List[Edge]]) -> Set[Ref]:
    """
    Return the set of reachable nodes from roots.
    edges_by_src is edges grouped by their `src`.
    """
    reachable: Set[Ref] = set()
    todo: Set[Ref] = set(roots)
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


def node_index_from_edges(edges: Iterable[Edge]) -> Dict[Ref, Node]:
    node_by_ref: Dict[Ref, Node] = {}

    def register_map(ref, node):
        value = node_by_ref.setdefault(ref, node)
        assert node == value
    for e in edges:
        register_map(e.src_ref, e.src)
        register_map(e.dest_ref, e.dest)

    return node_by_ref


def refs_from_nodes(nodes: Iterable[Node]) -> Set[Ref]:
    return {node.ref for node in nodes}


bead_index_from_edges = node_index_from_edges
refs_from_beads = refs_from_nodes


def refs_from_edges(edges: Iterable[Edge]) -> Set[Ref]:
    return set(node_index_from_edges(edges))


def toposort(edges: Sequence[Edge]) -> List[Node]:
    """
    Topological sort.
    """
    edges_by_dest = group_by_dest(edges)
    node_by_ref = node_index_from_edges(edges)

    todo = set(node_by_ref.keys())
    path: List[Ref] = []
    output: List[Node] = []

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
