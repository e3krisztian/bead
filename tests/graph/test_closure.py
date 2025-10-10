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
    # This documents the problem when input name differs from actual bead name
    # WITHOUT input_map: results in broken links (phantom node created)
    # WITH input_map: link is properly established
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
    # NOTE: 'd2', and 'e2' are NOT reachable because the input name "changed_input_name"
    # doesn't match the bead name "d", so a phantom node is created instead.


def test_input_name_and_actual_name_differs_with_input_map():
    # Same scenario as above, but WITH input_map to fix the broken link
    builder = GraphBuilder()
    builder.define('a1 b1 c1 d1 e1')
    builder.define('a2 b2 c2 d2 e2')
    builder.compile('a1 -> b1 -> c1 --> d1 -> e1')
    builder.compile('            c1 -:changed_input_name:-> d2')
    builder.compile('a2 -> b2 -> c2 --> d2 -> e2')

    # Fix the broken link: d2 has input named "changed_input_name", map it to bead "c"
    builder.map_input('d2', 'changed_input_name', 'c')

    graph = BeadGraph.from_beads(tuple(builder.beads))
    edges_by_src = group_by_src(graph.edges)

    reachable = closure(list(builder.ref_for('c1')), edges_by_src)

    # NOW d2 and e2 ARE reachable because input_map translated "changed_input_name" → "c"
    assert reachable == set(builder.ref_for('c1', 'd1', 'e1', 'd2', 'e2'))


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


def test_input_map_translates_names():
    """
    Test that input_map correctly translates input names to actual bead names.

    Scenario:
    - Bead 'data_raw' exists
    - Bead 'processor' has input named "input_data" that references data_raw
    - processor.input_map = {"input_data": "data_raw"}

    Expected:
    - Edge connects data_raw → processor (using mapped name for lookup)
    - Edge label is "input_data" (preserves original input name)
    - No phantom node created (bead found via mapped name)
    """
    builder = GraphBuilder()
    builder.define('d1 p1')  # data_raw and processor
    builder.compile('d1 -:input_data:-> p1')

    # Simulate renaming: input refers to "d1" by name "input_data", but actual bead is named "d"
    builder.map_input('p1', 'input_data', 'd')

    graph = BeadGraph.from_beads(tuple(builder.beads))

    # Should have 2 beads (no phantom)
    assert len(graph.beads) == 2

    # Should have 1 edge
    assert len(graph.edges) == 1
    edge = graph.edges[0]

    # Edge should connect d1 → p1
    assert edge.src.content_id == 'content_id_d1'
    assert edge.dest.content_id == 'content_id_p1'

    # Edge label should be original input name
    assert edge.label == 'input_data'


def test_phantom_node_gets_mapped_name():
    """
    Test that phantom nodes created for missing beads use the mapped name.

    Scenario:
    - Bead 'processor' has input named "data_input" referencing missing bead
    - processor.input_map = {"data_input": "raw_data"}

    Expected:
    - Phantom node is created with name "raw_data" (mapped name)
    - Edge label is "data_input" (original input name)
    - This allows the phantom to be resolved when "raw_data" bead appears
    """
    builder = GraphBuilder()
    builder.define('p1 x9')
    builder.compile('x9 -:data_input:-> p1')
    builder.phantom('x9')  # mark as phantom so it's not included in beads

    # Map the input name to the actual bead name
    builder.map_input('p1', 'data_input', 'raw_data')

    graph = BeadGraph.from_beads(tuple(builder.beads))

    # Should have 2 beads: p1 + phantom
    assert len(graph.beads) == 2

    # Find the phantom node
    phantom = [b for b in graph.beads if b.content_id == 'content_id_x9'][0]

    # Phantom should have the MAPPED name, not the input name
    assert phantom.name == 'raw_data'

    # Edge should still use the original input name as label
    assert len(graph.edges) == 1
    assert graph.edges[0].label == 'data_input'
    assert graph.edges[0].src.name == 'raw_data'
