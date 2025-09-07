from bead_cli.web.graph import Ref
from bead_cli.web.graph import closure
from bead_cli.web.graph import group_by_src
from bead_cli.web.graph import reverse
from bead_cli.web.sketch import Sketch
from tests.sketcher import Sketcher


def test_one_path():
    sketcher = Sketcher()
    sketcher.define('a1 b1 c1 d1 e1')
    sketcher.compile('a1 -> b1 -> c1 -> d1 -> e1')
    sketch = Sketch.from_beads(tuple(sketcher.beads))
    edges_by_src = group_by_src(sketch.edges)

    reachable = closure([Ref.from_bead(sketcher['c1'])], edges_by_src)

    assert reachable == set(sketcher.ref_for('c1', 'd1', 'e1'))


def test_two_paths():
    sketcher = Sketcher()
    sketcher.define('a1 b1 c1 d1 e1')
    sketcher.define('a2 b2 c2 d2 e2')
    sketcher.compile('a1 -> b1 -> c1 -> d1 -> e1')
    sketcher.compile('a2 -> b2 -> c2 -> d2 -> e2')
    sketch = Sketch.from_beads(tuple(sketcher.beads))
    edges_by_src = group_by_src(sketch.edges)

    reachable = closure(list(sketcher.ref_for('c1', 'c2')), edges_by_src)

    assert reachable == set(sketcher.ref_for('c1', 'd1', 'e1', 'c2', 'd2', 'e2'))


def test_input_name_and_actual_name_differs():
    # this should result in broken links on the graph now,
    # as by default both the name and kind of beads are taken into account when searching for upgrades
    # XXX/future: implement different input resolution strategies in graph output?
    sketcher = Sketcher()
    sketcher.define('a1 b1 c1 d1 e1')
    sketcher.define('a2 b2 c2 d2 e2')
    sketcher.compile('a1 -> b1 -> c1 --> d1 -> e1')
    sketcher.compile('            c1 -:changed_input_name:-> d2')
    sketcher.compile('a2 -> b2 -> c2 --> d2 -> e2')
    sketch = Sketch.from_beads(tuple(sketcher.beads))
    edges_by_src = group_by_src(sketch.edges)

    reachable = closure(list(sketcher.ref_for('c1')), edges_by_src)

    assert reachable == set(sketcher.ref_for('c1', 'd1', 'e1'))
    # NOTE: 'd2', and 'e2' should also be reachable, but changing the input name broke the link in Sketch.
    # Well, the link is still there, and the input would still be found by content_id alone in a live system,
    # however the input bead is considered missing as there is no bead with the input name,
    # worse still, as a result upgrade would not work for that input by default.


def test_loop():
    # it is an impossible bead config,
    # but in general loops should not cause problems to closure calculation
    sketcher = Sketcher()
    sketcher.define('a1 b1 c1')
    sketcher.compile('a1 -> b1 -> c1 -> a1')
    sketch = Sketch.from_beads(tuple(sketcher.beads))
    edges_by_src = group_by_src(sketch.edges)

    reachable = closure(list(sketcher.ref_for('b1')), edges_by_src)

    assert reachable == set(sketcher.ref_for('a1', 'b1', 'c1'))


def test_reverse():
    sketcher = Sketcher()
    sketcher.define('a1 b1 c1 d1 e1')
    sketcher.compile('a1 -> b1 -> c1 -> d1 -> e1')
    sketch = Sketch.from_beads(tuple(sketcher.beads))
    edges_by_src = group_by_src(reverse(sketch.edges))

    reachable = closure([Ref.from_bead(sketcher['c1'])], edges_by_src)

    assert reachable == set(sketcher.ref_for('a1', 'b1', 'c1'))
