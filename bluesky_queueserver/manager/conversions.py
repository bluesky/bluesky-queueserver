import math
import numpy as np
import pandas as pd
import re
import os
import ast

import logging

from .profile_ops import _convert_str_to_number

logger = logging.getLogger(__name__)


def simplify_plan_descriptions(plans_source):
    """
    Simplify plan representations in the list of allowed plans. Simplified representations
    may be more convenient for the web client applications. The conversion accurately supports
    only simple data types: ``int``, ``float``, ``str``, lists of ints, floats or strings
    (expressed as ``List[int]`` or ``typing.List[int]``), enums and lists of enums. Enum
    can be specified in custom description of plan parameter with enum values which are string
    or names of plans or devices.

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
        if "description" in p_items:
            plan["description"] = p_items["description"]

        if "parameters" in p_items and p_items["parameters"]:
            plan["parameters"] = []
            for param in p_items["parameters"]:
                p = dict()
                p["name"] = param["name"]  # "name" is mandatory
                p["kind"] = param["kind"]  # Always present in the description

                if "description" in param:
                    p["description"] = param["description"]

                # Annotation
                an = param.get("annotation", None)

                if an is not None:
                    p_type = an.get("type", None)

                    # Check if the parameter is enum
                    en = []
                    for kwd in ("devices", "plans", "enums"):
                        enums = an.get(kwd, None)
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
                        if p_type:
                            if re.search(r"\bfloat\b", p_type.lower()):
                                p["type"] = "float"
                            elif re.search(r"\bint\b", p_type.lower()):
                                p["type"] = "int"
                            elif re.search(r"\bstr\b", p_type.lower()):
                                p["type"] = "str"

                    # Determine if the parameter is list
                    if p_type:
                        if re.search(r"^typing.list\[.+]$", p_type.lower()) or re.search(
                            r"^list\[.+]$", p_type.lower()
                        ):
                            p["is_list"] = True
                        else:
                            p["is_list"] = False

                # Set the default value (optional) and decide if the parameter is optional

                if "default" in param:
                    try:
                        default = ast.literal_eval(param["default"])
                        p["default"] = default
                        p["is_optional"] = True
                    except Exception:
                        logger.error(f"Failed to reconstruct the default value {param['default']} from string")
                p["is_optional"] = "default" in p

                # Copy 'min', 'max' and 'step' values if any
                if "min" in param:
                    try:
                        p["min"] = _convert_str_to_number(param["min"])
                    except Exception as ex:
                        logger.error(f"Failed to reconstruct 'min' value from string: {ex}")
                if "max" in param:
                    try:
                        p["max"] = _convert_str_to_number(param["max"])
                    except Exception as ex:
                        logger.error(f"Failed to reconstruct 'max' value from string: {ex}")
                if "step" in param:
                    try:
                        p["step"] = _convert_str_to_number(param["step"])
                    except Exception as ex:
                        logger.error(f"Failed to reconstruct 'step' value from string: {ex}")

                plan["parameters"].append(p)

        plans_filtered.update({p_name: plan})

    return plans_filtered


def _is_nan(val):
    """
    Determine if the value is ``math.nan``. The function ``math.isnan()`` may be applied
    only to floating point numbers (definitely not to strings).
    """
    return isinstance(val, (np.floating, float)) and math.isnan(val)


def _read_cell_parameter(cell_value):
    """
    Returns
    -------
    Element read from the cell and converted to appropriate format or ``math.nan`` if element is missing
    """
    if isinstance(cell_value, str):
        return eval(cell_value, {}, {})

    elif _is_nan(cell_value):
        return math.nan

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


def spreadsheet_to_plan_list(*, spreadsheet_file, file_name, **kwargs):  # noqa: F821
    """
    The default function for converting spreadsheet to a list of plans. The function
    accepts files in .xlsx and .csv formats. For Excel files, only the first sheet of
    the workbook will be loaded. The spreadsheet is expected to be prepared using
    the following format (printed Pandas dataframe, NaN is an empty cell):

      plan_name                                   args   num  delay detectors
         'count'                      ['det1', 'det2']  10.0    NaN       NaN
         'count'                              ['det1']   NaN    0.5       NaN
         'count'                                   NaN   NaN    0.5  ['det1']
            NaN                                    NaN   NaN    NaN       NaN
          'scan'      ['det1', 'det2'], 'motor', -1, 1  10.0    NaN       NaN
          'scan'  ['det1', 'det2'], 'motor', -1, 1, 10   NaN    NaN       NaN
         'count'                              ['det2']   2.0    0.7       NaN
         'count'                              ['det2']   2.0    NaN       NaN

    Guidelines:

    - The first row contains column names. The names of columns 1 and 2 can be
      arbitrary, but should be kept informative. It is recommended to name column 2 as
      'args'. The remaining column names should strictly match names of plan kwargs.

    - Each of the remaining rows contains parameters of a single plan. Empty rows
      are permitted. The row is considered empty if the cell containing plan name
      is empty. In this case the remaining row cells are ignored.

    - Column 1 contains plan names. Plan names should be enclosed in quotation marks.
      (Plan name will be accepted without quotation marks but it is recommended to use
      quatation marks for consistency.) Plan names must satisfy requirements for Python
      identifiers, otherwise the exception will be raised. (The plan with the respective
      name should also exist in the RE Worker environment, but this function doesn't
      verify it.)

    - Column 2 contains plan args. The args should be comma-separated. The list of args
      should not be enclosed in ``[]`` (e.g. ['det1', 'det2'] is considered as one arg
      containing a list of detectors. The names of the devices (motors or detectors)
      should be enclosed in quotation marks. The cell may be left empty if the plan
      is not using args. NOTE: column 2 is reserved for args and it should be present
      even if no plans in the list accepts args.

    - Columns 3,4... contain values of kwargs. Each column is reserved for kwarg with
      the name listed in row 1. If a kwarg is not used for a given plan, the cell should
      be empty.

    The contents of the cells are evaluated using Python expression ``eval(<cell_value>, {}, {}``,
    therefore the expressions may not contain variable names or function calls. But they may
    represent arbitrary expressions containing numbers and strings (numbers and strings
    can be organized in lists, dictionaries, lists of dictionaries etc. If a cell contains
    unquoted device name (e.g. ``motor``), the name will be interpreted by ``eval()`` as
    a variable and an exception will be raised.

    Parameters
    ----------
    spreadsheet_file : file
        readable file object for '.xlsx' or '.csv' spreadsheet file.
    file_name : str
        name of the spreadsheet file. Extension of the file name is used to determine the file type.
    kwargs : dict
        absorbs kwargs that are not used by this function

    Returns
    -------
    list(dict)
        List of dictionaries containing plan parameters. Each dictionary contains elements ``name`` (str),
        ``args`` (list) and ``kwargs`` (dict).
    """
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
    n_rows = len(df[key_plan_name])

    def _format_row(df, nr):
        # Print the row as a tuple
        return str(tuple([df[_][nr] for _ in df.keys()]))

    plan_list = []

    for nr in range(n_rows):
        plan_name = df[key_plan_name][nr]

        # If plan name is missing or has False bool value, then just skip the line (probably it is empty)
        if not plan_name or _is_nan(plan_name):
            continue

        # The following step allows the plan names to be quoted or unquoted as well as to detect cases
        #   when name is of wrong type (list or dict) because spreadsheet formatting is incorrect.
        try:
            plan_name = _read_cell_parameter(plan_name)
        except Exception:
            # Exception is raised if the plan name is unquoted. If plan name is just a corrupt
            #   expression, then it will remain a string and the plan will be rejected by RE Manager.
            pass
        # Check if the plan name is string and it is formatted to represent a valid plan name
        if not isinstance(plan_name, str):
            raise ValueError(
                f"Plan name '{plan_name}' (row {nr + 1}: {_format_row(df, nr)}) has "
                f"incorrect type ('{type(plan_name)}'): only 'str' type is supported)"
            )
        if not plan_name.isidentifier():
            raise ValueError(
                f"Plan name '{plan_name}' (row {nr + 1}: {_format_row(df, nr)}) is not a valid plan name"
            )

        try:
            plan_args = []
            if key_plan_args:
                args_ss = df[key_plan_args][nr]
                if not _is_nan(args_ss):
                    args_ss = f"[{args_ss}]"  # args_ss will typically be a string anyway
                    plan_args = _read_cell_parameter(args_ss)

            plan_kwargs = {}
            for key in keys_kwargs:
                kwarg = _read_cell_parameter(df[key][nr])
                if not _is_nan(kwarg):
                    plan_kwargs[key] = kwarg

            plan = {"name": plan_name, "item_type": "plan"}
            if plan_args:
                plan["args"] = plan_args
            if plan_kwargs:
                plan["kwargs"] = plan_kwargs
            plan_list.append(plan)

        except Exception as ex:
            logger.exception(f"{ex}")
            raise ValueError(
                f"Error occurred while interpreting plan parameters in row {nr + 1} ({_format_row(df, nr)}): {ex}"
            )

    return plan_list
