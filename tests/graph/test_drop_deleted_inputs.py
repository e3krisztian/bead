from bead_cli.graph.bead_graph import BeadGraph
from tests.graph_builder import GraphBuilder
from tests.graph_builder import bead


def test_drop_deleted_inputs_removes_missing_edges():
    builder = GraphBuilder()
    builder.define('a1 b1 c1')
    builder.compile(
        """
        a1 -> b1
        a1 -:survivor:-> c1
        """
    )

    graph = builder.graph
    orig_beads = graph.beads
    graph = BeadGraph(
        beads=orig_beads,
        edges=tuple(e for e in graph.edges if e.label == 'survivor')
    )

    graph = graph.drop_deleted_inputs()
    assert orig_beads != graph.beads
    assert len(graph.edges) == 1
    assert len(bead(graph, 'a1').inputs) == 0
    assert len(bead(graph, 'b1').inputs) == 0
    assert len(bead(graph, 'c1').inputs) == 1
    assert bead(graph, 'c1').inputs[0].name == 'survivor'
