import os
import inspect
from ._common import copy_default_profile_collection, patch_first_startup_file
from bluesky_queueserver.manager.profile_tools import global_user_namespace
from bluesky_queueserver.manager.profile_ops import load_profile_collection


def create_local_imports_files(tmp_path):
    path_dir = os.path.join(tmp_path, "dir_local_imports")
    fln_func = os.path.join(path_dir, "file_func.py")
    fln_gen = os.path.join(path_dir, "file_gen.py")

    os.makedirs(path_dir, exist_ok=True)

    # Create file1
    code1 = """
from bluesky_queueserver.manager.profile_tools import set_user_ns

# Function that has the parameter 'ipython'
@set_user_ns
def f1(some_value, user_ns, ipython):
    user_ns["func_was_called"] = "func_was_called"
    return (some_value, user_ns["v_from_namespace"], bool(ipython))

# Function that has no parameter 'ipython'
@set_user_ns
def f1a(some_value, user_ns):
    user_ns["func_A_was_called"] = "func_was_called"
    return (some_value, user_ns["v_from_namespace"])

"""
    with open(fln_func, "w") as f:
        f.writelines(code1)

    # Create file2
    code2 = """
from bluesky_queueserver.manager.profile_tools import set_user_ns

# Function that has the parameter 'ipython'
@set_user_ns
def f2(some_value, user_ns, ipython):
    user_ns["gen_was_called"] = "gen_was_called"
    yield (some_value, user_ns["v_from_namespace"], bool(ipython))

# Function that has no parameter 'ipython'
@set_user_ns
def f2a(some_value, user_ns):
    user_ns["gen_A_was_called"] = "gen_was_called"
    yield (some_value, user_ns["v_from_namespace"])

@set_user_ns
def f3(some_value, user_ns, ipython):
    user_ns["value_f3"] = some_value

f3(91)

"""
    with open(fln_gen, "w") as f:
        f.writelines(code2)


patch_code = """
from dir_local_imports.file_func import f1, f1a
from dir_local_imports.file_gen import f2, f2a

from bluesky_queueserver.manager.profile_tools import set_user_ns

@set_user_ns
def f4(some_value, user_ns, ipython):
    user_ns["value_f4"] = some_value

f4(90)

"""


def test_set_user_ns_1(tmp_path):
    """
    Tests for ``set_user_ns`` decorator. The functionality of the decorator
    is fully tested (only without IPython):
    - using ``global_user_namespace`` to pass values in and out of the function
      defined in the imported module (emulation of ``get_ipython().user_ns``).
    - checking if the function is executed from IPython (only for the function
      defined in the imported module).
    """
    pc_path = copy_default_profile_collection(tmp_path)

    create_local_imports_files(pc_path)
    patch_first_startup_file(pc_path, patch_code)

    nspace = load_profile_collection(pc_path)
    assert len(nspace) > 0, "Failed to load the profile collection"
    assert "f1" in nspace, "Test for local imports failed"
    assert "f2" in nspace, "Test for local imports failed"

    # Test if the decorator `set_user_ns` does not change function type
    assert inspect.isgeneratorfunction(nspace["f1"]) is False
    assert inspect.isgeneratorfunction(nspace["f2"]) is True

    # Check if the extra arguments are removed from the function signature
    def check_signature(func):
        params = inspect.signature(func).parameters
        assert "user_ns" not in params
        assert "ipython" not in params

    check_signature(nspace["f1"])
    check_signature(nspace["f1a"])
    check_signature(nspace["f2"])
    check_signature(nspace["f2a"])

    assert nspace["value_f3"] == 91
    assert nspace["value_f4"] == 90

    # Test function
    global_user_namespace.set_user_namespace(user_ns=nspace, use_ipython=False)
    global_user_namespace.user_ns["v_from_namespace"] = "value-sent-to-func"
    assert nspace["v_from_namespace"] == "value-sent-to-func"

    result_func = nspace["f1"](60)
    assert nspace["func_was_called"] == "func_was_called"
    assert result_func[0] == 60
    assert result_func[1] == "value-sent-to-func"
    assert result_func[2] is False

    result_func = nspace["f1a"](65)
    assert nspace["func_A_was_called"] == "func_was_called"
    assert result_func[0] == 65
    assert result_func[1] == "value-sent-to-func"

    # Test generator
    global_user_namespace.user_ns["v_from_namespace"] = "value-sent-to-gen"
    result_func = list(nspace["f2"](110))[0]
    assert nspace["gen_was_called"] == "gen_was_called"
    assert result_func[0] == 110
    assert result_func[1] == "value-sent-to-gen"
    assert result_func[2] is False

    result_func = list(nspace["f2a"](115))[0]
    assert nspace["gen_A_was_called"] == "gen_was_called"
    assert result_func[0] == 115
    assert result_func[1] == "value-sent-to-gen"
