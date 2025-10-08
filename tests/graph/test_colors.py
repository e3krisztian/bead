import pytest

from bead_cli.graph.freshness import OUT_OF_DATE
from bead_cli.graph.freshness import PHANTOM
from bead_cli.graph.freshness import SUPERSEDED
from bead_cli.graph.freshness import UP_TO_DATE
from tests.sketcher import Sketcher
from tests.sketcher import bead


def test_new_version_marks_older_superseded():
    sketcher = Sketcher()
    sketcher.define('a1')

    graph = sketcher.graph
    graph.color_beads()

    assert bead(graph, 'a1').freshness == UP_TO_DATE

    sketcher = Sketcher()
    sketcher.define('a1 a2')

    graph = sketcher.graph
    graph.color_beads()

    assert bead(graph, 'a1').freshness == SUPERSEDED
    assert bead(graph, 'a2').freshness == UP_TO_DATE

    sketcher = Sketcher()
    sketcher.define('a1 a2 a3')

    graph = sketcher.graph
    graph.color_beads()

    assert bead(graph, 'a1').freshness == SUPERSEDED
    assert bead(graph, 'a2').freshness == SUPERSEDED
    assert bead(graph, 'a3').freshness == UP_TO_DATE


def test_unconnected():
    sketcher = Sketcher()
    sketcher.define('a1 a2 b1 c2')

    graph = sketcher.graph
    graph.color_beads()

    assert bead(graph, 'a1').freshness == SUPERSEDED
    assert bead(graph, 'a2').freshness == UP_TO_DATE
    assert bead(graph, 'b1').freshness == UP_TO_DATE
    assert bead(graph, 'c2').freshness == UP_TO_DATE


def test_up_to_date_input():
    sketcher = Sketcher()
    sketcher.define('a1 b1')
    sketcher.compile(
        """
        a1 -> b1
        """
    )

    graph = sketcher.graph
    graph.color_beads()

    assert bead(graph, 'a1').freshness == UP_TO_DATE
    assert bead(graph, 'b1').freshness == UP_TO_DATE

    sketcher = Sketcher()
    sketcher.define('a1 a2 b1')
    sketcher.compile(
        """
        a2 -> b1
        """
    )

    graph = sketcher.graph
    graph.color_beads()

    assert bead(graph, 'a1').freshness == SUPERSEDED

    assert bead(graph, 'a2').freshness == UP_TO_DATE
    assert bead(graph, 'b1').freshness == UP_TO_DATE


def test_out_of_date_input():
    sketcher = Sketcher()
    sketcher.define('a1 a2 b1')
    sketcher.compile(
        """
        a1 -> b1
        """
    )

    graph = sketcher.graph
    graph.color_beads()

    assert bead(graph, 'a1').freshness == SUPERSEDED
    assert bead(graph, 'b1').freshness == OUT_OF_DATE


def test_phantom_input():
    sketcher = Sketcher()
    sketcher.define('d1 e1')
    sketcher.phantom('d1')
    sketcher.compile(
        """
        d1 --> e1
        """
    )

    graph = sketcher.graph
    graph.color_beads()

    assert {'e'} == {bead.name for bead in sketcher.beads}
    assert len(list(sketcher.beads)) == 1
    assert len(graph.beads) == 2

    assert bead(graph, 'd1').freshness == PHANTOM
    assert bead(graph, 'e1').freshness == OUT_OF_DATE


def test_impossible_loop():
    # sketcher are always forming a DAG, so this can not happen
    sketcher = Sketcher()
    sketcher.define('a1 b1')
    sketcher.compile(
        """
        a1 -> b1
        b1 -> a1
        """
    )

    graph = sketcher.graph
    with pytest.raises(ValueError):
        graph.color_beads()


def test_coloring_is_transitive():
    sketcher = Sketcher()
    sketcher.define('a1    c1 d1 e1')
    sketcher.define('a2 b2 c2      ')
    sketcher.phantom('c1')
    sketcher.compile(
        """
        a1          c1 -> d1 -> e1
        a2 -> b2 -> c2
        """
    )

    graph = sketcher.graph
    graph.color_beads()

    assert bead(graph, 'a1').freshness == SUPERSEDED
    assert bead(graph, 'c1').freshness == PHANTOM
    assert bead(graph, 'd1').freshness == OUT_OF_DATE
    assert bead(graph, 'e1').freshness == OUT_OF_DATE

    assert bead(graph, 'a2').freshness == UP_TO_DATE
    assert bead(graph, 'b2').freshness == UP_TO_DATE
    assert bead(graph, 'c2').freshness == UP_TO_DATE
