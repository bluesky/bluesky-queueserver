import pandas as pd
import os


def spreadsheet_to_plan_list(*, spreadsheet_file, data_type, file_name, **kwargs):
    """
    Process trivial spreadsheet with plan parameters (for use in unit tests).
    The spreadsheet is expected to contain parameters of 'count' plan in the form:
        name   num   delay
    0   count  5     1
    1   count  6     0.5

    NOTE: this function returns item (plan) parameters without setting 'item_type'.
    Those items are considered plans (equivalent to ``item_type`` set as ``plan``).
    The items in the list may also be instructions (or other item types when they
    are supported). In this case each item should include ``item_type`` set explicitly.

    Parameters
    ----------
    spreadsheet_file : file
        readable file object
    data_type : str
        string that represents type of data in the spreadsheets. May be used to
        distinguish between different types of spreadsheets used at the same beamline.
    file_name : str
        name of the uploaded spreadsheet file, may be used to determine file type by
        looking at the extension.

    Returns
    -------
    plan_list : list(dict)
        Dictionary representing a list of plans extracted from the spreadsheet.
    """

    # Test that data type is successfully passed. In real processing functions we would
    #   check if it matches one of the supported functions.
    if data_type == "unsupported":
        raise ValueError(f"Unsupported data type: '{data_type}'")

    # If custom processing function (such as this one) returns None, then the spreadsheet
    #   is passed to the default processing function.
    if data_type == "process_with_default_function":
        return None

    # Check that file name has extension 'xlsx'. Raise the exception otherwise.
    ext = os.path.splitext(file_name)[1]
    if ext != ".xlsx":
        raise ValueError(f"Unsupported file (extension '{ext}')")

    df = pd.read_excel(spreadsheet_file, index_col=0, engine="openpyxl")
    plan_list = []
    n_rows, _ = df.shape
    for nr in range(n_rows):
        plan = {
            "name": df["name"][nr],
            "args": [["det1", "det2"]],
            "kwargs": {"num": int(df["num"][nr]), "delay": float(df["delay"][nr])},
        }
        plan_list.append(plan)
    return plan_list
