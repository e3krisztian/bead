import zipfile

import pytest

from bead.infra.fs import write_file
from bead.workspace import Workspace, _ZipCreator

TS = '20260601T000000000000+0000'


@pytest.fixture
def workspace(tmp_path):
    ws = Workspace(tmp_path / 'ws')
    ws.create('test-kind')
    write_file(ws.directory / 'code', 'code content')
    write_file(ws.directory / 'output/README', 'README content')
    return ws


def test_zip_creator_compress_false_uses_stored(tmp_path, workspace):
    zip_path = tmp_path / 'out.zip'
    _ZipCreator().create(zip_path, workspace, TS, 'test', compress=False)
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            assert info.compress_type == zipfile.ZIP_STORED, (
                f'{info.filename} has compress_type={info.compress_type}')


def test_zip_creator_compress_true_uses_deflated(tmp_path, workspace):
    zip_path = tmp_path / 'out.zip'
    _ZipCreator().create(zip_path, workspace, TS, 'test', compress=True)
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            assert info.compress_type == zipfile.ZIP_DEFLATED, (
                f'{info.filename} has compress_type={info.compress_type}')
