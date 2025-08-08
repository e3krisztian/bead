from bead_cli.web.sketch import Sketch
from tests.sketcher import Sketcher
from tests.sketcher import bead


def test_new_version_marks_older_superseded():
    sketcher = Sketcher()
    sketcher.define('a1 b1 c1')
    sketcher.compile(
        """
        a1 -> b1
        a1 -:survivor:-> c1
        """
    )

    sketch = sketcher.sketch
    orig_beads = sketch.beads
    sketch = Sketch(
        beads=orig_beads,
        edges=tuple(e for e in sketch.edges if e.label == 'survivor')
    )

    sketch = sketch.drop_deleted_inputs()
    assert orig_beads != sketch.beads
    assert len(sketch.edges) == 1
    assert len(bead(sketch, 'a1').inputs) == 0
    assert len(bead(sketch, 'b1').inputs) == 0
    assert len(bead(sketch, 'c1').inputs) == 1
    assert bead(sketch, 'c1').inputs[0].name == 'survivor'
