from . import persistence as m


def get_structure():
    return {
        '1': 'árvíztűrő',
        'a': 'tükörfúrógép',
        'z': [1, '', 3.14, {}]
    }


def test_streams(tmp_path):
    """Test persistence through file streams."""
    # given a persisted structure as a file
    file_path = tmp_path / 'file'
    original_structure = get_structure()
    serialized = m.dumps(original_structure)
    file_path.write_text(serialized)

    # when file is read back
    file_content = file_path.read_text()
    structure = m.loads(file_content)

    # then it equals the original structure
    assert original_structure == structure


def test_strings():
    """Test persistence through strings."""
    # given a persisted structure as a string
    original_structure = get_structure()
    string = m.dumps(original_structure)

    # when string is parsed back
    structure = m.loads(string)

    # then it equals the original structure
    assert original_structure == structure
