import pytest

from bead_cli.graph.freshness import OUT_OF_DATE
from bead_cli.graph.freshness import PHANTOM
from bead_cli.graph.freshness import SUPERSEDED
from bead_cli.graph.freshness import UP_TO_DATE
from tests.graph_builder import GraphBuilder
from tests.graph_builder import bead


def test_new_version_marks_older_superseded():
    builder = GraphBuilder()
    builder.define('a1')

    graph = builder.graph
    graph.color_beads()

    assert bead(graph, 'a1').freshness == UP_TO_DATE

    builder = GraphBuilder()
    builder.define('a1 a2')

    graph = builder.graph
    graph.color_beads()

    assert bead(graph, 'a1').freshness == SUPERSEDED
    assert bead(graph, 'a2').freshness == UP_TO_DATE

    builder = GraphBuilder()
    builder.define('a1 a2 a3')

    graph = builder.graph
    graph.color_beads()

    assert bead(graph, 'a1').freshness == SUPERSEDED
    assert bead(graph, 'a2').freshness == SUPERSEDED
    assert bead(graph, 'a3').freshness == UP_TO_DATE


def test_unconnected():
    builder = GraphBuilder()
    builder.define('a1 a2 b1 c2')

    graph = builder.graph
    graph.color_beads()

    assert bead(graph, 'a1').freshness == SUPERSEDED
    assert bead(graph, 'a2').freshness == UP_TO_DATE
    assert bead(graph, 'b1').freshness == UP_TO_DATE
    assert bead(graph, 'c2').freshness == UP_TO_DATE


def test_up_to_date_input():
    builder = GraphBuilder()
    builder.define('a1 b1')
    builder.compile(
        """
        a1 -> b1
        """
    )

    graph = builder.graph
    graph.color_beads()

    assert bead(graph, 'a1').freshness == UP_TO_DATE
    assert bead(graph, 'b1').freshness == UP_TO_DATE

    builder = GraphBuilder()
    builder.define('a1 a2 b1')
    builder.compile(
        """
        a2 -> b1
        """
    )

    graph = builder.graph
    graph.color_beads()

    assert bead(graph, 'a1').freshness == SUPERSEDED

    assert bead(graph, 'a2').freshness == UP_TO_DATE
    assert bead(graph, 'b1').freshness == UP_TO_DATE


def test_out_of_date_input():
    builder = GraphBuilder()
    builder.define('a1 a2 b1')
    builder.compile(
        """
        a1 -> b1
        """
    )

    graph = builder.graph
    graph.color_beads()

    assert bead(graph, 'a1').freshness == SUPERSEDED
    assert bead(graph, 'b1').freshness == OUT_OF_DATE


def test_phantom_input():
    builder = GraphBuilder()
    builder.define('d1 e1')
    builder.phantom('d1')
    builder.compile(
        """
        d1 --> e1
        """
    )

    graph = builder.graph
    graph.color_beads()

    assert {'e'} == {bead.name for bead in builder.beads}
    assert len(list(builder.beads)) == 1
    assert len(graph.beads) == 2

    assert bead(graph, 'd1').freshness == PHANTOM
    assert bead(graph, 'e1').freshness == OUT_OF_DATE


def test_impossible_loop():
    # builder always forms a DAG, so this can not happen
    builder = GraphBuilder()
    builder.define('a1 b1')
    builder.compile(
        """
        a1 -> b1
        b1 -> a1
        """
    )

    graph = builder.graph
    with pytest.raises(ValueError):
        graph.color_beads()


def test_coloring_is_transitive():
    builder = GraphBuilder()
    builder.define('a1    c1 d1 e1')
    builder.define('a2 b2 c2      ')
    builder.phantom('c1')
    builder.compile(
        """
        a1          c1 -> d1 -> e1
        a2 -> b2 -> c2
        """
    )

    graph = builder.graph
    graph.color_beads()

    assert bead(graph, 'a1').freshness == SUPERSEDED
    assert bead(graph, 'c1').freshness == PHANTOM
    assert bead(graph, 'd1').freshness == OUT_OF_DATE
    assert bead(graph, 'e1').freshness == OUT_OF_DATE

    assert bead(graph, 'a2').freshness == UP_TO_DATE
    assert bead(graph, 'b2').freshness == UP_TO_DATE
    assert bead(graph, 'c2').freshness == UP_TO_DATE
