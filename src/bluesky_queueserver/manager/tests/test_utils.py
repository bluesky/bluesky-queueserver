import copy
import re

import pytest

from bluesky_queueserver.manager.utils import filter_dict_by_permitted_keys, merge_dict_by_permitted_keys


@pytest.mark.parametrize(
    "d, permitted_keys, expected",
    [
        ({"a": 1, "b": 2, "c": 3}, ["/"], {"a": 1, "b": 2, "c": 3}),
        ({"a": 1, "b": 2, "c": 3}, ["/a"], {"a": 1}),
        ({"a": 1, "b": 2, "c": 3}, ["/a/"], {"a": 1}),
        ({"a": 1, "b": 2, "c": 3}, ["/b"], {"b": 2}),
        ({"a": 1, "b": 2, "c": 3}, ["/b/"], {"b": 2}),
        ({"a": 1, "b": 2, "c": 3}, ["/d"], {}),
        ({"a": 1, "b": 2, "c": 3}, ["/d/"], {}),
        ({"a": 1, "b": 2, "c": 3}, ["/a", "/b"], {"a": 1, "b": 2}),
        ({"a": 1, "b": 2, "c": 3}, ["/a/", "/b/"], {"a": 1, "b": 2}),
        ({"a": 1, "b": 2, "c": 3}, ["/a", "/b/"], {"a": 1, "b": 2}),
        ({"a": 1, "b": 2, "c": 3}, ["/a/", "/b"], {"a": 1, "b": 2}),
        ({"a": 1, "b": 2, "c": 3}, ["/a", "/d"], {"a": 1}),
        ({"a": 1, "b": 2, "c": 3}, ["/a/", "/d/"], {"a": 1}),
        ({"a": 1, "b": 2, "c": 3}, ["/a", "/d/"], {"a": 1}),
        ({"a": 1, "b": 2, "c": 3}, ["/a/", "/d"], {"a": 1}),
        ({"a": 1, "b": 2, "c": 3}, ["/", "/a/", "/d"], {"a": 1, "b": 2, "c": 3}),
        ({"a": {"b": 1, "c": 2}, "d": 2}, ["/"], {"a": {"b": 1, "c": 2}, "d": 2}),
        ({"a": {"b": 1, "c": 2}, "d": 2}, ["/a"], {"a": {"b": 1, "c": 2}}),
        ({"a": {"b": 1, "c": 2}, "d": 2}, ["/a/"], {"a": {"b": 1, "c": 2}}),
        ({"a": {"b": 1, "c": 2}, "d": 2}, ["/a/b"], {"a": {"b": 1}}),
        ({"a": {"b": 1, "c": 2}, "d": 2}, ["/a/b/"], {"a": {"b": 1}}),
        ({"a": {"b": 1, "c": 2}, "d": 2}, ["/a/c"], {"a": {"c": 2}}),
        ({"a": {"b": 1, "c": 2}, "d": 2}, ["/a/c/"], {"a": {"c": 2}}),
        ({"a": {"b": 1, "c": 2}, "d": 2}, ["/a/d"], {}),
        ({"a": {"b": 1, "c": 2}, "d": 2}, ["/a/d/"], {}),
    ],
)
def test_filter_dict_by_permitted_keys(d, permitted_keys, expected):
    assert filter_dict_by_permitted_keys(d, permitted_keys) == expected


# fmt: off
@pytest.mark.parametrize("d, updates, permitted_keys, expected, expected_paths", [
    ({"a": 1, "b": 2}, {"a": 10}, ["/"], {"a": 10}, ["/a"]),
    ({"a": 1, "b": 2}, {"a": 10}, ["/a"], {"a": 10}, ["/a"]),
    ({"a": 1, "b": 2}, {"a": 10}, ["/a/"], {"a": 10}, ["/a"]),
    ({"a": 1, "b": 2}, {"c": 10}, ["/c"], {"c": 10}, ["/c"]),
    ({"a": 1, "b": 2}, {"a": 10, "c": 3}, ["/a", "/c"], {"a": 10, "c": 3}, ["/a", "/c"]),
    ({"a": 1}, {}, ["/a"], {}, []),
    # Permitted path: the whole value is replaced
    ({"a": {"b": 1, "c": 2}}, {"a": {"b": 10}}, ["/a"], {"a": {"b": 10}}, ["/a"]),
    # Nested permitted path: nested dictionaries are merged
    ({"a": {"b": 1, "c": 2}}, {"a": {"b": 10}}, ["/a/b"], {"a": {"b": 10, "c": 2}}, ["/a/b"]),
    ({"a": {"b": {"x": 1}, "c": 2}}, {"a": {"b": {"y": 2}}}, ["/a/b"], {"a": {"b": {"y": 2}, "c": 2}}, ["/a/b"]),
    ({"a": {"b": {"x": 1}, "c": 2}}, {"a": {"b": {"y": 2}}}, ["/a/b/y"],
     {"a": {"b": {"x": 1, "y": 2}, "c": 2}}, ["/a/b/y"]),
    ({}, {"a": {"b": 10}}, ["/a/b"], {"a": {"b": 10}}, ["/a/b"]),
])
# fmt: on
def test_merge_dict_by_permitted_keys_01(d, updates, permitted_keys, expected, expected_paths):
    """
    ``merge_dict_by_permitted_keys``: successful updates. The original dictionary is not modified.
    """
    d_copy = copy.deepcopy(d)
    assert merge_dict_by_permitted_keys(d, updates, permitted_keys) == (expected, expected_paths)
    assert d == d_copy


# fmt: off
@pytest.mark.parametrize("d, updates, permitted_keys, msg", [
    ({"a": 1}, {"a": 10}, [], "['/a'] is not permitted"),
    ({"a": 1}, {"a": 10}, ["/b"], "['/a'] is not permitted"),
    ({"a": 1}, {"a": 10}, [""], "['/a'] is not permitted"),
    ({"a": 1}, {"a": 10}, ["a"], "['/a'] is not permitted"),
    ({"a": 1}, {"a": 10}, ["/b", ""], "['/a'] is not permitted"),
    ({"a": 1}, {"a": 10, "b": 2, "c": 3}, ["/b"], "['/a', '/c'] is not permitted"),
    ({"a": 1}, {"ab": 10}, ["/a"], "['/ab'] is not permitted"),
    ({"a": {"b": 1}}, {"a": 10}, ["/a/b"], "['/a'] is not permitted"),
    ({"a": {"b": 1}}, {"a": {}}, ["/a/b"], "['/a'] is not permitted"),
    ({"a": {"b": 1}}, {"a": {"b": 2, "c": 3}}, ["/a/b"], "['/a/c'] is not permitted"),
    ({"a": 5}, {"a": {"b": 2}}, ["/a/b"], "['/a'] is not permitted"),
    ({"a": 1}, {"a/b": 10}, ["/"], "Invalid metadata key 'a/b'"),
    ({"a": 1}, {"": 10}, ["/"], "Invalid metadata key ''"),
    ({}, {"a": {1: 10}}, ["/a/b"], "Invalid metadata key 1"),
    ({"a": 1}, [("a", 10)], ["/"], "Metadata updates must be a dictionary"),
])
# fmt: on
def test_merge_dict_by_permitted_keys_02_fail(d, updates, permitted_keys, msg):
    """
    ``merge_dict_by_permitted_keys``: invalid or non-permitted updates.
    """
    with pytest.raises(ValueError, match=re.escape(msg)):
        merge_dict_by_permitted_keys(d, updates, permitted_keys)
