'''
Functions to persist python structures or load them.
'''

import io
import json

# json is used for serializing objects for persistence as it is
# - in the standard library from >=2.6 (including 3.*)
# - 2.7 version decodes strings as unicode, unlike e.g. the csv module
# - the interface is also becoming the standard one for serialization,
#   e.g. the competing plistlib's interface was deprecated in 3.4 in
#   favor of a new json-like one


ReadError = json.JSONDecodeError


JSON_SAVE_OPTIONS = dict(
    indent=4,
    sort_keys=True,
    ensure_ascii=True,
)


def load(istream):
    return json.load(istream)


def loads(string):
    return json.loads(string)


def dumps(content):
    return json.dumps(content, **JSON_SAVE_OPTIONS)


def dump(content, ostream):
    json.dump(content, ostream, **JSON_SAVE_OPTIONS)


def zip_load(zipfile, path: str):
    with zipfile.open(path) as f:
        return load(io.TextIOWrapper(f, encoding='utf-8'))


def zip_dump(content, zipfile, path: str):
    with zipfile.open(path, 'w') as f:
        return dump(content, io.TextIOWrapper(f, encoding='utf-8'))


def file_load(path):
    with open(path) as f:
        return load(f)


def file_dump(content, path):
    with open(path, 'w') as f:
        dump(content, f)
