import copy

import pytest

from bluesky_queueserver.manager.logging_setup import PPrintForLogging as ppfl

msg_in_01a = 10
msg_out_01a = "10"

msg_in_01b = "Some string"
msg_out_01b = "'Some string'"

msg_in_01c = [1, 2, 3, "ab"]
msg_out_01c = "[1, 2, 3, 'ab']"

msg_in_01d = {"ab": 10, "cd": "Some string"}
msg_out_01d = "{'ab': 10, 'cd': 'Some string'}"

# Complicated dictionary, which should be passed without change
# fmt: off
msg_in_02 = {
    "short_list": [1, 2, 3, 4, 5],
    "small_dict": {
        "number": 50,
        "str": "Another string",
        "short_dict": {
            "number": 70,
            "short_list": ["ab", "cd", "ef", {"gh": 50, "ef": "abc"}],
        }
    },
    "str": "This is some string",
    "number": 10,
}
# fmt: on

msg_out_02 = """{'short_list': [1, 2, 3, 4, 5],
 'small_dict': {'number': 50,
                'str': 'Another string',
                'short_dict': {'number': 70,
                               'short_list': ['ab',
                                              'cd',
                                              'ef',
                                              {'gh': 50, 'ef': 'abc'}]}},
 'str': 'This is some string',
 'number': 10}"""

msg_in_03a = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
msg_out_03a = "[1, 2, 3, 4, 5, '...']"

# fmt: off
msg_in_03b = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8, "i": 9, "j": 10, "k": 11, "l": 12}
# fmt: on
msg_out_03b = "{'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 5, 'f': 6, 'g': 7, '...': '...'}"


msg_in_04a = {"b": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], "a": "Some string"}
msg_out_04a = "{'b': [1, 2, 3, 4, 5, '...'], 'a': 'Some string'}"

msg_in_04b = [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], "Some string"]
msg_out_04b = "[[1, 2, 3, 4, 5, '...'], 'Some string']"

msg_in_04c = [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
msg_out_04c = "[[1, 2, 3, 4, 5, '...']]"

# fmt: off
msg_in_04d = [{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8}, "Some String"]
# fmt: on
msg_out_04d = """[{'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 5, 'f': 6, 'g': 7, '...': '...'},
 'Some String']"""

msg_in_05a = {
    "success": True,
    "msg": "",
    "item": {
        "name": "func_receive_data",
        "args": [
            [
                0.9369286739807017,
                0.34376855785565996,
                0.42682410595184694,
                0.07561990556676668,
                0.99047480979217,
                0.6645094767060512,
                0.23949066214689685,
                0.9506291248522921,
                0.074846613000778,
                0.5056064487856279,
                0.5857598003946347,
                0.3171865307386059,
                0.2749306001778441,
                0.14193313428663756,
                0.8694254472618858,
            ]
        ],
        "kwargs": {},
    },
}

msg_out_05a = """{'success': True,
 'msg': '',
 'item': {'name': 'func_receive_data',
          'args': [[0.9369286739807017,
                    0.34376855785565996,
                    0.42682410595184694,
                    0.07561990556676668,
                    0.99047480979217,
                    '...']],
          'kwargs': {}}}"""

# msg: off
msg_in_06a = {"plans_existing": {"plan1": {}, "plan2": []}, "devices_allowed": []}
# msg: on
msg_out_06a = "{'plans_existing': {'plan1': '{...}', 'plan2': '{...}'}, 'devices_allowed': []}"

# Long string: 508 characters
msg_in_07a = "The result is an aggregate list of returned values."
msg_out_07a = "'The result is an aggregate list of returned values.'"

msg_in_07b = "If all awaitables are completed successfully, the result is an aggregate list of returned values."
msg_out_07b = "'If all awaitables are completed  ...\\n... gregate list of returned values.'"

msg_in_07c = {
    "msg": "If all awaitables are completed successfully, the result is an aggregate list of returned values.",
    "lst": ["If all awaitables are completed successfully, the result is an aggregate list of returned values."],
}

msg_out_07c = """{'msg': 'If all awaitables are completed  ...\\n'
        '... gregate list of returned values.',
 'lst': ['If all awaitables are completed  ...\\n'
         '... gregate list of returned values.']}"""

msg_in_07d = {
    "traceback": "If all awaitables are completed successfully, "
    "the result is an aggregate list of returned values.",
}

msg_out_07d = """{'traceback': 'If all awaitables are completed successfully, the result is an '
              'aggregate list of returned values.'}"""


# fmt: on
@pytest.mark.parametrize(
    "msg_in, msg_out",
    [
        (msg_in_01a, msg_out_01a),
        (msg_in_01b, msg_out_01b),
        (msg_in_01c, msg_out_01c),
        (msg_in_01d, msg_out_01d),
        (msg_in_02, msg_out_02),
        (msg_in_03a, msg_out_03a),
        (msg_in_03b, msg_out_03b),
        (msg_in_04a, msg_out_04a),
        (msg_in_04b, msg_out_04b),
        (msg_in_04c, msg_out_04c),
        (msg_in_04d, msg_out_04d),
        (msg_in_05a, msg_out_05a),
        (msg_in_06a, msg_out_06a),
        (msg_in_07a, msg_out_07a),
        (msg_in_07b, msg_out_07b),
        (msg_in_07c, msg_out_07c),
        (msg_in_07d, msg_out_07d),
    ],
)
# fmt: off
def test_PPrintForLogging_01(msg_in, msg_out):
    msg_in_backup = copy.deepcopy(msg_in)
    result = str(ppfl(msg_in, max_list_size=5, max_dict_size=7, max_chars_in_str=64))
    print(result)
    # Check the result
    assert result == msg_out
    # Check that the orinal data did not change
    assert msg_in_backup == msg_in
