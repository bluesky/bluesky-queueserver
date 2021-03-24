import math
import numpy as np
import pandas as pd
import re
import os


def filter_plan_descriptions(plans_source):
    """
    Convert descriptions of the allowed plans to satisfy requirements of the web client.
    The conversion accurately supports only simple data types: ``int``, ``float``, ``str``,
    lists of ints, floats or strings (expressed as ``List[int]`` or ``typing.List[int]``),
    enums and lists of enums. Enum can be specified in custom description of plan
    parameter with enum values which are string or names of plans or devices.

    The limitation on processed types is based on potential capabilities of the web client.
    Complex type hints may also be processed correctly if the result could be expressed
    in terms of the supported types. The performance of the conversion function must
    be checked with the plans using complex types before deployment.

    The converted plan parameters represent the following dictionary:

    .. code-block:: python

        {
            "name": "<plan_name>",  # required
            "description": "<plan_description>",  # optional
            "parameters": {
                "name": "<parameter_name>",  # required
                "description": "<parameter_description>",  # optional
                "kind": <parameter_kind>,  # e.g. POSITIONAL_OR_KEYWORD, optional
                "type": <parameter_type>,  # "int", "float" or "str", optional
                "enum": ["..", "..", ".."],  # List of enum values (strings) if the parameter is enum, optional
                "is_list": True,  # True or False, optional
                "default": <default_value>,  # Default value (if available), optional
                "is_optional": True,  # True or False, indicates if the parameter is optional, optional
                "min": <min_parameter_value>,  # optional
                "max": <max_parameter_value>,  # optional
                "step": <step_value>,  # optional
            },
        }

    Missing ``type`` means that type is unknown. If the parameter is enum, then ``type`` is
    always ``str``. Missing ``is_list`` means that it is unknown if the parameter accepts a list.
    The parameter is optional (``is_optional is True``) if the default value is specified.
    Step value may be specified in the custom parameter annotation if the value is discrete
    and changes from ``min`` to ``max`` value with fixed ``step``.

    Parameters
    ----------
    plan_source : dict
        Dictionary of plans: key - plan name, value - plan parameters.

    Returns
    -------
    dict
        Dictionary of plans with reduced set of parameters.
    """

    plans_filtered = {}
    for p_name, p_items in plans_source.items():

        plan = dict()
        # Plan name - mandatory (actually it always equals 'pname')
        plan["name"] = p_items["name"]
        # "description" is optional, don't include empty description.
        if "description" in p_items and p_items["description"]:
            plan["description"] = p_items["description"]

        if "parameters" in p_items and p_items["parameters"]:
            plan["parameters"] = []
            for param in p_items["parameters"]:
                p = dict()

                # "name" is mandatory
                p["name"] = param["name"]

                # "kind" is optional, but it is always present in the description
                if "kind" in param:
                    p["kind"] = param["kind"]

                # Get description
                desc = None
                if "custom" in param:
                    desc = param["custom"].get("description", None)
                if not desc:
                    desc = param.get("description", None)
                if desc:
                    p["description"] = desc

                # Choose the parameter annotation
                an = None
                if "custom" in param:
                    an = param["custom"].get("annotation", None)
                if not an:
                    an = param.get("annotation", None)

                # Check if the parameter is enum
                en = []
                for kwd in ("devices", "plans", "enums"):
                    if "custom" in param:
                        enums = param["custom"].get(kwd, None)
                        if enums:
                            for v in enums.values():
                                if not isinstance(v, (list, tuple)):
                                    v = [v]
                                en.extend(v)
                if en:
                    # Parameter is enum, so the type is 'str'
                    p["type"] = "str"
                    p["enum"] = en
                else:
                    # Otherwise try to determine type
                    if an:
                        if re.search(r"\bfloat\b", an.lower()):
                            p["type"] = "float"
                        elif re.search(r"\bint\b", an.lower()):
                            p["type"] = "int"
                        elif re.search(r"\bstr\b", an.lower()):
                            p["type"] = "str"

                # Determine if the parameter is list
                if an:
                    if re.search(r"^typing.list\[.+]$", an.lower()) or re.search(r"^list\[.+]$", an.lower()):
                        p["is_list"] = True
                    else:
                        p["is_list"] = False

                # Set the default value (optional) and decide if the parameter is optional
                if "default" in param:
                    p["default"] = param["default"]
                    p["is_optional"] = True
                else:
                    p["is_optional"] = False

                # Copy 'min', 'max' and 'step' values if any
                if "min" in param:
                    p["min"] = param["min"]
                if "max" in param:
                    p["max"] = param["max"]
                if "step" in param:
                    p["step"] = param["step"]

                plan["parameters"].append(p)

        plans_filtered.update({p_name: plan})

    return plans_filtered


def _read_cell_parameter(cell_value):
    """"""
    if isinstance(cell_value, str):
        return eval(cell_value, {}, {})

    elif isinstance(cell_value, (np.floating, float)):
        # The following is necessary because ints in excel spreadsheet will likely be represented as np.float64.
        #   So check if they round well and return 'int' if they do.
        if float(int(cell_value)) == float(cell_value):
            return int(cell_value)
        else:
            return float(cell_value)

    elif isinstance(cell_value, (np.integer, int)):
        return int(cell_value)

    else:
        raise ValueError(
            f"Cell value '{cell_value}' has unsupported type '{type(cell_value)}': "
            "supported types: (int, float, str)"
        )


def spreadsheet_to_plan_list(*, spreadsheet_file, file_name, data_type, **kwargs):  # noqa: F821

    # Check if the spreadseet type has supported extension
    _, ss_ext = os.path.splitext(file_name)
    supported_ext = (".xlsx", ".xls", ".csv")
    if ss_ext not in supported_ext:
        raise ValueError(
            f"File '{file_name}' (extension '{ss_ext}') is not supported: supported extensions '{supported_ext}'"
        )

    if ss_ext == ".xlsx":
        df = pd.read_excel(spreadsheet_file, engine="openpyxl")
    elif ss_ext == ".xls":
        df = pd.read_excel(spreadsheet_file)
    elif ss_ext == ".csv":
        df = pd.read_csv(spreadsheet_file)

    column_keys = df.keys()
    if len(column_keys) < 1:
        raise ValueError(
            f"Spreadsheet is expected to have at least 1 column (plan name): {len(column_keys)} columns"
        )
    key_plan_name = column_keys[0]
    key_plan_args = column_keys[1] if (len(column_keys) > 1) else None
    keys_kwargs = column_keys[2:]  # This could be empty

    # Check that the columns for kwargs contain no duplicate names
    if len(set([_.lower() for _ in keys_kwargs])) != len(keys_kwargs):
        raise ValueError(f"Some plan kwarg names are identical: {keys_kwargs}")

    # The number of rows (rows are potentially plans, there could be empty rows though)
    n_rows = len(df["key_plan_name"])

    plan_list = []

    for nr in n_rows:
        plan_name = df[key_plan_name][nr]

        # If plan name is missing or has False bool value, then just skip the line (probably it is empty)
        if not plan_name or math.isnan(plan_name):
            continue

        # Check if the plan name is string and it is formatted to represent a valid plan name
        if not isinstance(plan_name, str):
            raise ValueError(
                f"Plan name '{plan_name}' (row {nr}) is of incorrect type ('{type(plan_name)}'): "
                "supported type 'str')"
            )
        if not plan_name.isidentifier():
            raise ValueError(f"Plan name '{plan_name}' (row {nr}) is not a valid plan name")

        try:
            plan_args = []
            if key_plan_args:
                plan_args = list(_read_cell_parameter(df[key_plan_args][nr]))

            plan_kwargs = {}
            for key in keys_kwargs:
                kwarg = _read_cell_parameter(df[key][nr])
                plan_kwargs[key] = kwarg

            plan_list.append({"plan": {"name": plan_name, "args": plan_args, "kwargs": plan_kwargs}})

        except Exception as ex:
            raise ValueError(f"Error occurred while interpreting plan parameters in row {nr}: {ex}")

    return plan_list
