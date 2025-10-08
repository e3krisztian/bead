import re

import pytest

from bead.infra.fs import read_file
from bead.infra.fs import rmtree
from bead.infra.fs import write_file
from bead.workspace import Workspace
from bead_cli.graph.sketch import BeadGraph
from tests.sketcher import Sketcher
from tests.graph.test_graphviz import needs_dot
from .test_helpers import create_bead_family


def create_bead_with_inputs_for_graph(shell, box, times, tmp_path_factory):
    """Create a bead with inputs specifically for graph command testing.

    This is a convenience function that creates the standard set of beads
    used by graph command tests.
    """
    # Create input beads
    create_bead_family(box, 'web_bead_a', [times.TS1], tmp_path_factory)
    create_bead_family(box, 'web_bead_b', [times.TS2], tmp_path_factory)

    # Create bead with inputs
    shell.bead('new', 'web_with_inputs')
    shell.cd('web_with_inputs')
    content = f'web_with_inputs_{times.TS3}'
    shell.write_file('README', content)
    shell.write_file('output/README', content)

    # Add inputs
    shell.bead('input', 'add', 'input_a', 'web_bead_a')
    shell.bead('input', 'add', 'input_b', 'web_bead_b')

    with shell.environment:
        box.store(Workspace('.'), times.TS3)

    shell.cd('..')
    shell.bead('discard', 'web_with_inputs')


def test_dot_output(shell, box, check, times, tmp_path_factory):
    create_bead_with_inputs_for_graph(shell, box, times, tmp_path_factory)
    shell.bead('graph dot all.dot')
    assert (shell.cwd / 'all.dot').exists()


@needs_dot
def test_svg_output(shell, box, check, times, tmp_path_factory):
    create_bead_with_inputs_for_graph(shell, box, times, tmp_path_factory)
    shell.bead('graph svg all.svg')
    assert (shell.cwd / 'all.svg').exists()


@needs_dot
def test_png_output(shell, box, check, times, tmp_path_factory):
    create_bead_with_inputs_for_graph(shell, box, times, tmp_path_factory)
    shell.bead('graph png all.png')
    assert (shell.cwd / 'all.png').exists()


def test_meta_save_load(shell, box, check, times, tmp_path_factory):
    create_bead_with_inputs_for_graph(shell, box, times, tmp_path_factory)

    shell.bead('graph save all.web')
    assert (shell.cwd / 'all.web').exists()

    shell.bead('graph dot all.dot')
    orig_web_dot = read_file(shell.cwd / 'all.dot')

    assert 'web_bead_a' in orig_web_dot
    assert 'web_bead_b' in orig_web_dot
    assert 'web_with_inputs' in orig_web_dot

    # destroy everything, except the meta files
    write_file(shell.cwd / 'all.dot', '')
    shell.bead('box', 'forget', box.name)
    rmtree(box.directory)

    shell.bead('graph load all.web dot all.dot')
    meta_web_dot = read_file(shell.cwd / 'all.dot')

    assert orig_web_dot == meta_web_dot


def test_heads_only(shell, box, check, times, tmp_path_factory):
    create_bead_family(box, 'web_history_bead', [times.TS1, times.TS2, times.TS3, times.TS4, times.TS5], tmp_path_factory)

    shell.bead('graph dot all.dot heads dot heads-only.dot')
    full_web = read_file(shell.cwd / 'all.dot')
    heads_only_web = read_file(shell.cwd / 'heads-only.dot')

    assert len(heads_only_web) < len(full_web)


def test_invalid_command_reported(shell):
    shell.bead('graph load x this-command-does-not-exist c', expect_failure=True)
    assert 'ERROR' in shell.stderr
    assert re.search('.ould not .*parse', shell.stderr)
    assert str(['this-command-does-not-exist', 'c']) in shell.stderr




@pytest.fixture
def sketch(shell):
    sketcher = Sketcher()
    sketcher.define('a1 b1 c1 d1 e1 f1')
    sketcher.compile(
        """
        a1 -> b1 -> c1 -> d1

              b1 ------------> e1 -> f1
        """
    )
    sketcher.graph.to_file(shell.cwd / 'computation.web')


@pytest.fixture
def indirect_links_sketch(shell):
    sketcher = Sketcher()
    sketcher.define('a1 b1 b2 c1 c2 d3 e1 f1')
    sketcher.compile(
        """
        a1 -> b1               e1 -> f1
              b2 -> c1
                    c2 -> d3
        """
    )
    sketcher.graph.to_file(shell.cwd / 'computation.web')


def test_filter_no_args_no_filtering(shell, sketch):
    shell.bead('graph load computation.web / .. / save filtered.web')

    assert shell.read_file('computation.web') == shell.read_file('filtered.web')


def test_filter_sources_through_cluster_links(shell, indirect_links_sketch):
    shell.bead('graph load computation.web / b .. / save filtered.web')

    graph = BeadGraph.from_file(shell.cwd / 'filtered.web')
    assert graph.cluster_by_name.keys() == set('bcd')
    assert len(graph.cluster_by_name['c']) == 2


def test_filter_sinks_through_cluster_links(shell, indirect_links_sketch):
    shell.bead('graph load computation.web / .. c / save filtered.web')

    graph = BeadGraph.from_file(shell.cwd / 'filtered.web')
    assert graph.cluster_by_name.keys() == set('abc')
    assert len(graph.cluster_by_name['c']) == 2


def test_filter(shell, sketch):
    shell.bead('graph load computation.web / b c .. c f / save filtered.web')
    graph = BeadGraph.from_file(shell.cwd / 'filtered.web')
    assert graph.cluster_by_name.keys() == set('bcef')


def test_filter_filtered_out_sink(shell, indirect_links_sketch):
    # f1 is unreachable from sources {b, c}, so it will be not a reachable sink
    shell.bead('graph load computation.web / b c .. c f / save filtered.web')

    graph = BeadGraph.from_file(shell.cwd / 'filtered.web')
    assert graph.cluster_by_name.keys() == set('bc')
