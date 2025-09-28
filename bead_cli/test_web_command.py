import re

import pytest

from bead.tech.fs import read_file
from bead.tech.fs import rmtree
from bead.tech.fs import write_file
from bead_cli.web.sketch import Sketch
from tests.sketcher import Sketcher
from tests.web.test_graphviz import needs_dot


def test_dot_output(shell, bead_with_inputs):
    shell.bead('web dot all.dot')
    assert (shell.cwd / 'all.dot').exists()


@needs_dot
def test_svg_output(shell, bead_with_inputs):
    shell.bead('web svg all.svg')
    assert (shell.cwd / 'all.svg').exists()


@needs_dot
def test_png_output(shell, bead_with_inputs):
    shell.bead('web png all.png')
    assert (shell.cwd / 'all.png').exists()


def test_meta_save_load(shell, bead_with_inputs, box):
    shell.bead('web save all.web')
    assert (shell.cwd / 'all.web').exists()

    shell.bead('web dot all.dot')
    orig_web_dot = read_file(shell.cwd / 'all.dot')

    assert 'bead_a' in orig_web_dot
    assert 'bead_b' in orig_web_dot
    assert bead_with_inputs in orig_web_dot

    # destroy everything, except the meta files
    write_file(shell.cwd / 'all.dot', '')
    shell.bead('box', 'forget', box.name)
    rmtree(box.directory)

    shell.bead('web load all.web dot all.dot')
    meta_web_dot = read_file(shell.cwd / 'all.dot')

    assert orig_web_dot == meta_web_dot


def test_heads_only(shell, bead_with_history):
    shell.bead('web dot all.dot heads dot heads-only.dot')
    full_web = read_file(shell.cwd / 'all.dot')
    heads_only_web = read_file(shell.cwd / 'heads-only.dot')

    assert len(heads_only_web) < len(full_web)


def test_invalid_command_reported(shell):
    shell.bead('web load x this-command-does-not-exist c', expect_failure=True)
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
    sketcher.sketch.to_file(shell.cwd / 'computation.web')


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
    sketcher.sketch.to_file(shell.cwd / 'computation.web')


def test_filter_no_args_no_filtering(shell, sketch):
    shell.bead('web load computation.web / .. / save filtered.web')

    assert shell.read_file('computation.web') == shell.read_file('filtered.web')


def test_filter_sources_through_cluster_links(shell, indirect_links_sketch):
    shell.bead('web load computation.web / b .. / save filtered.web')

    sketch = Sketch.from_file(shell.cwd / 'filtered.web')
    assert sketch.cluster_by_name.keys() == set('bcd')
    assert len(sketch.cluster_by_name['c']) == 2


def test_filter_sinks_through_cluster_links(shell, indirect_links_sketch):
    shell.bead('web load computation.web / .. c / save filtered.web')

    sketch = Sketch.from_file(shell.cwd / 'filtered.web')
    assert sketch.cluster_by_name.keys() == set('abc')
    assert len(sketch.cluster_by_name['c']) == 2


def test_filter(shell, sketch):
    shell.bead('web load computation.web / b c .. c f / save filtered.web')
    sketch = Sketch.from_file(shell.cwd / 'filtered.web')
    assert sketch.cluster_by_name.keys() == set('bcef')


def test_filter_filtered_out_sink(shell, indirect_links_sketch):
    # f1 is unreachable from sources {b, c}, so it will be not a reachable sink
    shell.bead('web load computation.web / b c .. c f / save filtered.web')

    sketch = Sketch.from_file(shell.cwd / 'filtered.web')
    assert sketch.cluster_by_name.keys() == set('bc')
