import pandas as pd


def spreadsheet_to_plan_list(*, spreadsheet_file, **kwargs):
    """
    Process trivial spreadsheet with plan parameters (for use in unit tests).
    The spreadsheet is expected to contain parameters of 'count' plan in the form:
        name   num   delay
    0   count  5     1
    1   count  6     0.5

    Parameters
    ----------
    spreadsheet_file : file
        readable file object

    Returns
    -------
    plan_list : list(dict)
        Dictionary representing a list of plans extracted from the spreadsheet.
    """
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
