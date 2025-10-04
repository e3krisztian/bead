import os
import warnings
import zipfile

import pytest

from bead import layouts
from bead.infra.fs import write_file
from bead.workspace import Workspace

from .test_shell import Shell

# timestamps
TS1 = '20150901T151015000001+0200'
TS2 = '20150901T151016000002+0200'
TS3 = '20150901T151017000003+0200'
TS4 = '20150901T151018000004+0200'
TS5 = '20150901T151019000005+0200'
TS_LAST = TS5


class Times:
    """Container for timestamp constants."""
    TS1 = TS1
    TS2 = TS2
    TS3 = TS3
    TS4 = TS4
    TS5 = TS5
    TS_LAST = TS_LAST


@pytest.fixture
def times():
    """Fixture providing access to timestamp constants."""
    return Times()


class CheckAssertions:
    """Helper class for test assertions."""

    def __init__(self, shell):
        self.shell = shell

    def loaded(self, input_nick, readme_content):
        """
        Fail if an incorrect bead was loaded.

        Test beads are assumed to have different README-s, so the test goes by the expected value
        of the README.
        """
        readme_path = self.shell.cwd / f'input/{input_nick}/README'
        assert readme_path.exists(), f"README file not found at {readme_path}"
        content = readme_path.read_text()
        assert readme_content in content, f"Expected '{readme_content}' in README content: {content}"

    def not_loaded(self, input_nick):
        readme_path = self.shell.cwd / f'input/{input_nick}/README'
        assert not readme_path.exists(), f"README file should not exist at {readme_path}"


@pytest.fixture
def shell():
    """
    I am a shell user with a box
    """
    with Shell() as shell_instance:
        box_dir = shell_instance.cwd / 'box'
        os.makedirs(box_dir)
        shell_instance.bead('box', 'add', 'box', box_dir)
        yield shell_instance


@pytest.fixture
def box(shell):
    with shell.environment as env:
        return env.get_box('box')




@pytest.fixture
def check(shell):
    """Fixture providing assertion helpers."""
    return CheckAssertions(shell)




@pytest.fixture
def hacked_bead(tmp_path_factory):
    hacked_bead_path = tmp_path_factory.mktemp('hacked') / 'hacked_bead.zip'
    workspace_dir = tmp_path_factory.mktemp('workspace') / 'hacked_bead'
    ws = Workspace(workspace_dir)
    ws.create('hacked-kind')
    write_file(ws.directory / 'code', 'code')
    write_file(ws.directory / 'output/README', 'README')
    ws.pack(hacked_bead_path, TS1, comment='hacked bead')
    with zipfile.ZipFile(hacked_bead_path, 'a') as z:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            # this would cause a warning from zipfile for duplicate
            # name in zip file (which is perfectly valid, though hacky)
            z.writestr(f'{layouts.Archive.CODE}/code', 'HACKED')
            z.writestr(f'{layouts.Archive.DATA}/README', 'HACKED')
    return hacked_bead_path


