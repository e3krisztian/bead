import pytest

from bead_cli.web.io import loads, read_beads, write_beads
from bead_cli.web.freshness import Freshness


META_JSON = """\
[
    {
        "@class": "Dummy",
        "@encoding": "attrs",
        "box_name": "box",
        "content_id": "id_ood2",
        "freeze_time_str": "20190321T191922693711+0100",
        "freshness": {
            "@class": "Freshness",
            "@encoding": "enum",
            "value": "SUPERSEDED"
        },
        "inputs": [
            {
                "@class": "InputSpec",
                "@encoding": "attrs",
                "content_id": "id_ood1",
                "freeze_time_str": "20190321T191922693711+0100",
                "kind": "kind_ood1",
                "name": "ood1"
            },
            {
                "@class": "InputSpec",
                "@encoding": "attrs",
                "content_id": "id_root2_utd",
                "freeze_time_str": "20190321T191922693711+0100",
                "kind": "kind_root_2",
                "name": "root2"
            }
        ],
        "kind": "kind_ood2",
        "name": "ood2"
    },
    {
        "@class": "Dummy",
        "@encoding": "attrs",
        "box_name": "box",
        "content_id": "id_ood1",
        "freeze_time_str": "20190321T191922693711+0100",
        "freshness": {
            "@class": "Freshness",
            "@encoding": "enum",
            "value": "UP_TO_DATE"
        },
        "inputs": [
            {
                "@class": "InputSpec",
                "@encoding": "attrs",
                "content_id": "id_root1_ood",
                "freeze_time_str": "20180321T191922693711+0100",
                "kind": "kind_root_1",
                "name": "root"
            }
        ],
        "kind": "kind_ood1",
        "name": "ood1"
    },
    {
        "@class": "Dummy",
        "@encoding": "attrs",
        "box_name": "",
        "content_id": "id_root2_utd",
        "freeze_time_str": "20190321T191922693711+0100",
        "freshness": {
            "@class": "Freshness",
            "@encoding": "enum",
            "value": "OUT_OF_DATE"
        },
        "inputs": [],
        "kind": "kind_root_2",
        "name": "root2"
    },
    {
        "@class": "Dummy",
        "@encoding": "attrs",
        "box_name": "",
        "content_id": "id_root1_utd",
        "freeze_time_str": "20190321T191922693711+0100",
        "freshness": {
            "@class": "Freshness",
            "@encoding": "enum",
            "value": "SUPERSEDED"
        },
        "inputs": [],
        "kind": "kind_root_1",
        "name": "root1"
    },
    {
        "@class": "Dummy",
        "@encoding": "attrs",
        "box_name": "",
        "content_id": "id_root1_ood",
        "freeze_time_str": "20180321T191922693711+0100",
        "freshness": {
            "@class": "Freshness",
            "@encoding": "enum",
            "value": "SUPERSEDED"
        },
        "inputs": [],
        "kind": "kind_root_1",
        "name": "root1"
    },
    {
        "@class": "Dummy",
        "@encoding": "attrs",
        "box_name": "",
        "content_id": "id_ood3",
        "freeze_time_str": "20190321T191922693711+0100",
        "freshness": {
            "@class": "Freshness",
            "@encoding": "enum",
            "value": "UP_TO_DATE"
        },
        "inputs": [
            {
                "@class": "InputSpec",
                "@encoding": "attrs",
                "content_id": "id_ood2",
                "freeze_time_str": "20190321T191922693711+0100",
                "kind": "kind_ood2",
                "name": "ood2"
            },
            {
                "@class": "InputSpec",
                "@encoding": "attrs",
                "content_id": "id_phantom",
                "freeze_time_str": "20140321T191922693711+0100",
                "kind": "kind_ood2",
                "name": "phantom"
            }
        ],
        "kind": "kind_ood3",
        "name": "ood3"
    }
]
"""


def test_freshness():
    test_beads = loads(META_JSON)

    beads_by_name = {b.name: b for b in test_beads}
    assert beads_by_name['ood2'].freshness == Freshness.SUPERSEDED
    assert beads_by_name['ood1'].freshness == Freshness.UP_TO_DATE
    assert beads_by_name['root2'].freshness == Freshness.OUT_OF_DATE


def test_written_data_is_unchanged(tmp_path):
    new_meta = tmp_path / 'new_meta'
    write_beads(new_meta, loads(META_JSON))

    assert new_meta.read_text().splitlines() == META_JSON.splitlines()


def test_files(tmp_path):
    meta = tmp_path / 'new_meta'

    test_beads = loads(META_JSON)
    write_beads(meta, test_beads)
    beads_read_back = read_beads(meta)
    assert test_beads == beads_read_back


def test_missing_file(tmp_path):
    meta = tmp_path / 'nonexisting_file'

    with pytest.raises(FileNotFoundError):
        read_beads(meta)
