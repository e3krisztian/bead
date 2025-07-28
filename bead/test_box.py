import pytest
from .box import Box
from .tech.fs import write_file
from .tech.timestamp import time_from_user
from .workspace import Workspace


@pytest.fixture
def box(tmp_path_factory):
    """Create a test box with sample beads."""
    tmp_path = tmp_path_factory.mktemp('box')
    box = Box('test', tmp_path)

    def add_bead(name, kind, freeze_time):
        ws = Workspace(tmp_path / name)
        ws.create(kind)
        box.store(ws, freeze_time)

    add_bead('bead1', 'test-bead1', '20160704T000000000000+0200')
    add_bead('bead2', 'test-bead2', '20160704T162800000000+0200')
    add_bead('BEAD3', 'test-bead3', '20160704T162800000001+0200')
    return box


@pytest.fixture
def timestamp():
    """Provide a test timestamp."""
    return time_from_user('20160704T162800000001+0200')


def test_all_beads(box):
    """Test that all beads are returned."""
    bead_names = {b.name for b in box.all_beads()}
    assert {'bead1', 'bead2', 'BEAD3'} == bead_names


def test_find_with_uppercase_name(box, timestamp):
    """Test finding beads with uppercase names."""
    bead = box.search().by_name('BEAD3').at_or_older(timestamp).newest()
    assert 'BEAD3' == bead.name


def test_box_methods_tolerate_junk_in_box(tmp_path_factory):
    """Test that box methods work even with junk files present."""
    temp_dir = tmp_path_factory.mktemp("box_junk")
    box = Box('test', temp_dir)

    def add_bead(name, kind, freeze_time):
        ws = Workspace(temp_dir / name)
        ws.create(kind)
        box.store(ws, freeze_time)

    add_bead('bead1', 'test-bead1', '20160704T000000000000+0200')
    add_bead('bead2', 'test-bead2', '20160704T162800000000+0200')
    add_bead('BEAD3', 'test-bead3', '20160704T162800000001+0200')

    # add junk
    junk_file = box.directory / 'some-non-bead-file'
    write_file(junk_file, 'random bits')

    bead_names = {b.name for b in box.all_beads()}
    assert {'bead1', 'bead2', 'BEAD3'} == bead_names
