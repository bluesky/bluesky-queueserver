import math
import numpy as np
import os
import pandas as pd


def create_excel_file_from_plan_list(tmp_path, *, plan_list, ss_filename="spreadsheet", ss_ext=".xlsx"):
    """
    Create test spreadsheet file in temporary directory. Return full path to the spreadsheet
    and the expected list of plans with parameters. Supporting '.xlsx' and '.csv' extensions.
    The idea was to also write tests for '.xls' file support, but Pandas writer does not seem to
    support writing ".xls" files.

    Parameters
    ----------
    tmp_path : str
        temporary path
    plan_list : list(dict)
        list of plan parameters. Plan parameters may contain "item_type", but it will be ignored.
    ss_filename : str
        spreadsheet file name without extension
    ss_ext : str
        spreadsheet extension ('.xlsx' and '.csv' extensions are supported)

    Returns
    -------
    str
        full path to spreadsheet file
    """
    # Create sample Excel file
    ss_filename = ss_filename + ss_ext
    ss_path = os.path.join(tmp_path, ss_filename)

    def format_cell(val):
        if isinstance(val, (np.integer, int)):
            return int(val)
        elif isinstance(val, (np.floating, float)):
            return float(val)
        elif isinstance(val, str):
            return f"'{val}'"
        else:
            return str(val)

    plan_params = []
    kwarg_names = []
    for plan in plan_list:
        pp = [format_cell(plan["name"])]
        args_appended = False
        if "args" in plan:
            # Args are typically going to be represented as a printed list not enclosed in []
            plan_args = plan["args"]
            if not isinstance(plan_args, (tuple, list)):
                raise ValueError(
                    f"Plan args must be a tuple or a list: type(plan_args)={type(plan_args)} plan={plan}"
                )
            plan_args = list(plan_args)  # In case 'plan_args' is a tuple
            if plan_args:  # Do not
                plan_args_format = format_cell(plan_args)
                plan_args_format = plan_args_format[1:-1]  # Remove [ and ]
                pp.append(plan_args_format)
                args_appended = True
        if not args_appended:
            pp.append(math.nan)
        if "kwargs" in plan:
            plan_kwargs = plan["kwargs"].copy()
            for kw in kwarg_names:
                if kw in plan_kwargs:
                    pp.append(format_cell(plan_kwargs[kw]))
                    del plan_kwargs[kw]
                else:
                    pp.append(math.nan)
            for kw in plan_kwargs.keys():
                kwarg_names.append(kw)
                pp.append(format_cell(plan_kwargs[kw]))
        plan_params.append(pp)

    # Make all parameter lists equal length
    max_params = max([len(_) for _ in plan_params])
    for pp in plan_params:
        for _ in range(len(pp), max_params):
            pp.append(math.nan)

    col_names = ["plan_name", "args"] + kwarg_names

    def create_excel(ss_path, plan_params, col_names):
        df = pd.DataFrame(plan_params)
        df = df.set_axis(col_names, axis=1)

        _, ext = os.path.splitext(ss_path)
        if ext == ".xlsx":
            df.to_excel(ss_path, index=False, engine="openpyxl")
        elif ext == ".csv":
            df.to_csv(ss_path, index=False)
        return df

    def str_to_number(v):
        v = float(v)
        if int(v) == v:
            v = int(v)
        return v

    def fix_dataframe_types(df):
        """
        Fix types in dataframes loaded from .csv files: if any element in a column is 'str',
        the whole column is interpreted as 'str'. Convert strings that are numbers to 'int' and 'float' types.
        Correct types are important here to compare saved and restored dataframes.
        """
        # If any number in the column has type string, then the whole column is a string
        for key in df.keys():
            for n in range(len(df[key])):
                try:
                    df[key][n] = str_to_number(df[key][n])
                except Exception:
                    pass
        return df

    def verify_excel(ss_path, df):
        _, ext = os.path.splitext(ss_path)
        if ext == ".xlsx":
            df_read = pd.read_excel(ss_path, engine="openpyxl")
        elif ext == ".csv":
            df_read = pd.read_csv(ss_path)
            df_read = fix_dataframe_types(df_read)
        assert df_read.equals(df), str(df_read)

    df = create_excel(ss_path, plan_params, col_names)
    verify_excel(ss_path, df)

    return ss_path


# Sample list that contains working plans. May be used to create a spreadsheet.
# Plan with '"name": math.nan' will generate an empty line in the spreadsheet.
plan_list_sample = [
    {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 10}, "item_type": "plan"},
    {"name": "count", "args": [["det1"]], "kwargs": {"delay": 0.5}, "item_type": "plan"},
    {"name": "count", "kwargs": {"detectors": ["det1"], "delay": 0.5}, "item_type": "plan"},
    {"name": math.nan},
    {"name": "scan", "args": [["det1", "det2"], "motor", -1, 1], "kwargs": {"num": 10}, "item_type": "plan"},
    {"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10], "item_type": "plan"},
    {"name": "count", "args": [["det2"]], "kwargs": {"num": 2, "delay": 0.7}, "item_type": "plan"},
    {"name": "count", "args": [["det2"]], "kwargs": {"num": 2}, "item_type": "plan"},
]
