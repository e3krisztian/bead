from bead_cli.graph.graph import Ref
from bead_cli.graph.graph import closure
from bead_cli.graph.graph import group_by_src
from bead_cli.graph.graph import reverse
from bead_cli.graph.bead_graph import BeadGraph
from tests.graph_builder import GraphBuilder


def test_one_path():
    builder = GraphBuilder()
    builder.define('a1 b1 c1 d1 e1')
    builder.compile('a1 -> b1 -> c1 -> d1 -> e1')
    graph = BeadGraph.from_beads(tuple(builder.beads))
    edges_by_src = group_by_src(graph.edges)

    reachable = closure([Ref.from_bead(builder['c1'])], edges_by_src)

    assert reachable == set(builder.ref_for('c1', 'd1', 'e1'))


def test_two_paths():
    builder = GraphBuilder()
    builder.define('a1 b1 c1 d1 e1')
    builder.define('a2 b2 c2 d2 e2')
    builder.compile('a1 -> b1 -> c1 -> d1 -> e1')
    builder.compile('a2 -> b2 -> c2 -> d2 -> e2')
    graph = BeadGraph.from_beads(tuple(builder.beads))
    edges_by_src = group_by_src(graph.edges)

    reachable = closure(list(builder.ref_for('c1', 'c2')), edges_by_src)

    assert reachable == set(builder.ref_for('c1', 'd1', 'e1', 'c2', 'd2', 'e2'))


def test_input_name_and_actual_name_differs():
    # this should result in broken links on the graph now,
    # as by default both the name and kind of beads are taken into account when searching for upgrades
    # XXX/future: implement different input resolution strategies in graph output?
    builder = GraphBuilder()
    builder.define('a1 b1 c1 d1 e1')
    builder.define('a2 b2 c2 d2 e2')
    builder.compile('a1 -> b1 -> c1 --> d1 -> e1')
    builder.compile('            c1 -:changed_input_name:-> d2')
    builder.compile('a2 -> b2 -> c2 --> d2 -> e2')
    graph = BeadGraph.from_beads(tuple(builder.beads))
    edges_by_src = group_by_src(graph.edges)

    reachable = closure(list(builder.ref_for('c1')), edges_by_src)

    assert reachable == set(builder.ref_for('c1', 'd1', 'e1'))
    # NOTE: 'd2', and 'e2' should also be reachable, but changing the input name broke the link in BeadGraph.
    # Well, the link is still there, and the input would still be found by content_id alone in a live system,
    # however the input bead is considered missing as there is no bead with the input name,
    # worse still, as a result upgrade would not work for that input by default.


def test_loop():
    # it is an impossible bead config,
    # but in general loops should not cause problems to closure calculation
    builder = GraphBuilder()
    builder.define('a1 b1 c1')
    builder.compile('a1 -> b1 -> c1 -> a1')
    graph = BeadGraph.from_beads(tuple(builder.beads))
    edges_by_src = group_by_src(graph.edges)

    reachable = closure(list(builder.ref_for('b1')), edges_by_src)

    assert reachable == set(builder.ref_for('a1', 'b1', 'c1'))


def test_reverse():
    builder = GraphBuilder()
    builder.define('a1 b1 c1 d1 e1')
    builder.compile('a1 -> b1 -> c1 -> d1 -> e1')
    graph = BeadGraph.from_beads(tuple(builder.beads))
    edges_by_src = group_by_src(reverse(graph.edges))

    reachable = closure([Ref.from_bead(builder['c1'])], edges_by_src)

    assert reachable == set(builder.ref_for('a1', 'b1', 'c1'))
