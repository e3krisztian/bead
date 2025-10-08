"""
Test handling of duplicate beads (same content_id) across multiple boxes.

When the same bead exists in multiple boxes, the graph should handle it gracefully
without crashing. Since beads are content-addressed, duplicates should be deduplicated.
"""
from bead.meta import InputName
from bead.meta import InputSpec
from bead_cli.graph.node import Node
from bead_cli.graph.sketch import BeadGraph


def test_duplicate_bead_same_name_different_boxes():
    """
    Test that same bead (name + content_id) from different boxes doesn't crash.

    Scenario:
    - Box A contains: ("analysis", "content_abc")
    - Box B contains: ("analysis", "content_abc") - exact duplicate

    Expected behavior:
    - Should deduplicate to single bead in graph
    - Should not crash when creating edges
    - Should not crash during validation

    This reproduces the crash from error_20250818T184948419411+0200.txt:
    AssertionError in node_index_from_edges when same Ref maps to different Node instances.
    """
    # Create two separate Node instances with same name and content_id but different box_name
    # (simulating loading the same bead from different boxes)
    bead_from_box_a = Node(
        name="analysis",
        content_id="abc123",
        kind="computation",
        freeze_time_str="20200101T120000000000+0000",
        box_name='box_a',
    )

    bead_from_box_b = Node(
        name="analysis",
        content_id="abc123",
        kind="computation",
        freeze_time_str="20200101T120000000000+0000",
        box_name='box_b',
    )

    # Both beads in the list (as would happen when loading from multiple boxes)
    beads = [bead_from_box_a, bead_from_box_b]

    # This should not crash (used to crash with AssertionError in node_index_from_edges)
    graph = BeadGraph.from_beads(beads)

    # Should have deduplicated to single bead
    assert len(graph.beads) == 1

    # The deduplicated bead should have one of the box names
    assert graph.beads[0].box_name in ('box_a', 'box_b')


def test_duplicate_bead_with_inputs():
    """
    Test that duplicate beads (the ones WITH inputs) don't create conflicting edges.

    Scenario:
    - Bead "data" is loaded once
    - Bead "analysis" (with "data" as input) exists in BOTH box_a and box_b
    - Both copies of "analysis" loaded

    Expected behavior:
    - "analysis" bead should be deduplicated
    - Should not crash due to edges from both analysis copies

    This is the critical test case that triggers the AssertionError:
    When generate_input_edges processes both analysis_from_box_a and analysis_from_box_b,
    it creates edges with DIFFERENT Node instances as destinations (both with same Ref).
    The assertion in node_index_from_edges fails because the same Ref maps to different objects.
    """
    # Create input bead
    data_bead = Node(
        name="data",
        content_id="data_content_id",
        kind="raw",
        freeze_time_str="20200101T100000000000+0000",
        box_name='box_shared',
    )

    # Create analysis bead that exists in two boxes
    analysis_from_box_a = Node(
        name="analysis",
        content_id="analysis_content_id",
        kind="computation",
        freeze_time_str="20200101T120000000000+0000",
        inputs=[
            InputSpec(
                name=InputName("data"),
                content_id="data_content_id",
                kind="raw",
                freeze_time_str="20200101T100000000000+0000",
            )
        ],
        box_name='box_a',
    )

    analysis_from_box_b = Node(
        name="analysis",
        content_id="analysis_content_id",
        kind="computation",
        freeze_time_str="20200101T120000000000+0000",
        inputs=[
            InputSpec(
                name=InputName("data"),
                content_id="data_content_id",
                kind="raw",
                freeze_time_str="20200101T100000000000+0000",
            )
        ],
        box_name='box_b',
    )

    # Load all beads including duplicate analysis bead
    beads = [data_bead, analysis_from_box_a, analysis_from_box_b]

    # This should not crash (triggers AssertionError when bug present)
    # The bug: generate_input_edges is called for BOTH analysis beads,
    # creating edges: (data -> analysis_from_box_a) and (data -> analysis_from_box_b)
    # Both edges have dest_ref=("analysis", "analysis_content_id") but different dest objects
    graph = BeadGraph.from_beads(beads)

    # Should have deduplicated analysis bead
    assert len(graph.beads) == 2  # data + analysis

    # Should have one edge from data to analysis
    assert len(graph.edges) == 1
    assert graph.edges[0].label == 'data'
