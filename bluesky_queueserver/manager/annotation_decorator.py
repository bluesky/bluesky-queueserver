# ====================================================================================
#        The contents of this file are expected to be moved to Bluesky
# ====================================================================================

import functools
import inspect
import jsonschema
import textwrap
import pprint

_parameter_annotation_schema = {
    "type": "object",
    "required": ["description", "parameters"],
    "properties": {
        "description": {"type": "string"},
    },
    "additionalProperties": {
        "parameters": {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "required": ["description"],
                "properties": {
                    "description": {"type": "string"},
                },
                "additionalProperties": {
                    "annotation": {"type": "string"},
                    "devices": {
                        "type": "array",
                        "items": [{"type": "string"}],
                        "additionalItems": {"type": "string"},
                    },
                    "plans": {
                        "type": "array",
                        "items": [{"type": "string"}],
                        "additionalItems": {"type": "string"},
                    },
                },
            },
        },
        "returns": {
            "type": "object",
            "required": ["description"],
            "properties": {
                "description": {"type": "string"},
            },
            "additionalProperties": {
                "annotation": {"type": "string"},
            },
        },
    },
}


def _print_docstring_title(title):
    """
    Print title of the docstring to a string. Title is underline using correct number
    of ``-`` sybmols.

    Parameters
    ----------
    title: str
        Title (such as ``Parameters``, ``Returns`` or ``Yields``.

    Returns
    -------
    str
        String that contains the printed title. ``\n`` is always included.
    """
    return f"\n{title}\n{'-' * len(title)}\n"


def _print_parameter_name_and_type(p_name, p_type):
    """
    Print parameter name and type following Numpy docstring standard.
    If `p_name` is an empty string, then return an empty string.

    Parameters
    ----------
    p_name: str
        Parameter name
    p_type: str
        Parameter type
    """
    s = ""
    if p_name:
        s = f"{p_name}"
        if p_type:
            s += f": {p_type}"
    return f"{s}\n"


def _print_indented_block(text, indent=0, text_width=80):
    """
    Formats the block of text by wrapping it to the desired width and indenting it
    by inserting the desired number of spaces.

    Parameters
    ----------
    text: str
        Text to format
    indent: int
        The number of spaces to insert at the beginning of each line.
    text_width: int
        The maximum width of the text block including the leading spaces.

    Returns
    -------
    str
        The string that contains the formatted block.
    """
    text = textwrap.wrap(text.strip(), width=text_width - indent)
    text = "\n".join(text)
    if text and (text[-1] != "\n"):
        text += "\n"
    return textwrap.indent(text, " " * indent)


def _collect_data_for_docstring(func, annotation):
    """
    Collect data to be printed in docstring. The data is collected from
    custom annotation (dictionary passed as a parameter for the decorator)
    and standard Python annotations for the parameters (if any). Data from
    custom annotation always overrides Python parameter annotations.

    Parameters
    ----------
    func: callable
        Reference to the function.
    annotation: dict
        Custom annotation.

    Returns
    -------
    Dictionary of the collected parameters
    """
    signature = inspect.signature(func)
    parameters = signature.parameters
    return_annotation = signature.return_annotation

    doc_params = dict()
    # Description of the function (it must be present in annotation)
    doc_params["description"] = annotation["description"]
    # Flag that tells if the function is generator. Title for returning
    #   values for generator is 'Yields' and for regular functions it is 'Returns'
    doc_params["is_generator"] = inspect.isgeneratorfunction(func)

    if parameters:  # The function may have no parameters
        doc_params["parameters"] = dict()

        # We will print names of ALL parameters from the signature
        for p_name, p in parameters.items():
            # Select description, annotation and types from available sources.
            #   Annotation (parameter of the wrapper) always overrides Python annotation.
            doc_params["parameters"][p_name] = {}

            kind = p.kind.name
            kind = kind.lower().replace("_", " ")
            doc_params["parameters"][p_name]["kind"] = kind

            desc, an, plans, devices = "", "", {}, {}
            if ("parameters" in annotation) and (p_name in annotation["parameters"]):
                p_an = annotation["parameters"][p_name]
                desc = p_an["description"]  # Description MUST be there
                if "annotation" in p_an:
                    an = p_an["annotation"]
                    # Ignore annotation if it is an empty string. Lists of plans
                    #   and devices make no sense, so don't include them.
                    if an:
                        # Now save the lists of plans and devices if any
                        plans = p_an.get("plans", {})
                        devices = p_an.get("devices", {})

            if not an and parameters[p_name].annotation != inspect.Parameter.empty:
                an = str(parameters[p_name].annotation)

            doc_params["parameters"][p_name]["annotation"] = an
            doc_params["parameters"][p_name]["description"] = desc
            doc_params["parameters"][p_name]["plans"] = plans
            doc_params["parameters"][p_name]["devices"] = devices

            if p.default != inspect.Parameter.empty:
                # Print will print strings in quotes (desired behavior)
                v_default = pprint.pformat(p.default)
            else:
                v_default = None
            # If 'v_default' is None, it is not specified, so it should not be printed
            #   in the docstring at all
            doc_params["parameters"][p_name]["default"] = v_default

        # Print return value annotation and description. Again the annotation from
        #   custom annotation overrides Python annotation.
        doc_params["returns"] = {}
        desc, an = "", ""
        if "returns" in annotation or (return_annotation != inspect.Parameter.empty):
            if "returns" in annotation:
                desc = annotation["returns"]["description"]
                an = annotation["returns"].get("annotation", "")
            if not an:
                if return_annotation != inspect.Signature.empty:
                    an = str(return_annotation)
        doc_params["returns"]["description"] = desc
        doc_params["returns"]["annotation"] = an

    return doc_params


def _format_docstring(doc_params):
    """
    Print docstring to a string following Numpy docstring conventions.

    Parameters
    ----------
    doc_params: dict
        Dictionary containing docstring parameters. The dictionary should be created using
        function ``_collect_data_for_docstring()``.

    Returns
    -------
    str
        Formatted docstring.
    """
    text_width = 80  # Text width for the docsting
    not_documented_str = "WE ARE SORRY, THE ITEM IS NOT DOCUMENTED YET ..."
    tab_size = 4

    doc = f"{doc_params['description']}\n"
    # The function may have no parameters
    if doc_params["parameters"]:
        doc += _print_docstring_title("Parameters")

        for p_name, p in doc_params["parameters"].items():
            doc += _print_parameter_name_and_type(p_name, p["annotation"])

            # Insert the description of the parameter
            desc = p["description"]
            desc = desc if desc else not_documented_str
            doc += _print_indented_block(desc, indent=tab_size, text_width=text_width)

            if p["plans"]:
                doc += _print_indented_block("Allowed plan names:", indent=tab_size, text_width=text_width)
                for plan_group_name, plan_list in p["plans"].items():
                    s = f"'{plan_group_name}': {plan_list}"
                    doc += _print_indented_block(s, indent=tab_size * 2, text_width=text_width)

            if p["devices"]:
                doc += _print_indented_block("Allowed device names:", indent=tab_size, text_width=text_width)
                for device_group_name, device_list in p["devices"].items():
                    s = f"'{device_group_name}': {device_list}"
                    doc += _print_indented_block(s, indent=tab_size * 2, text_width=text_width)

            s = f"Kind: {p['kind']}"
            doc += _print_indented_block(s, indent=tab_size, text_width=text_width)

            if p["default"] is not None:
                s = f"Default: {p['default']}"
                doc += _print_indented_block(s, indent=tab_size, text_width=text_width)

        # Print return value type and description
        if doc_params["returns"]["annotation"] or doc_params["returns"]["description"]:
            title = "Yields" if doc_params["is_generator"] else "Returns"
            doc += _print_docstring_title(title)

            offset = 0
            s = doc_params["returns"]["annotation"]
            if s:
                doc += _print_indented_block(s, indent=tab_size, text_width=text_width)
                offset = 1

            s = doc_params["returns"]["description"]
            if s:
                doc += _print_indented_block(s, indent=tab_size * offset, text_width=text_width)

    return doc


def parameter_annotation_decorator(annotation):
    def function_wrap(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        jsonschema.validate(instance=annotation, schema=_parameter_annotation_schema)

        sig = inspect.signature(func)
        parameters = sig.parameters

        param_unknown = []
        for p in annotation["parameters"]:
            if p not in parameters:
                param_unknown.append(p)
        if param_unknown:
            msg = (
                f"Custom annotation parameters {param_unknown} are not "
                f"the signature of function '{func.__name__}'."
            )
            raise ValueError(msg)
        setattr(wrapper, "_custom_parameter_annotation_", annotation)

        # Create a docstring from the annotation if the function does not have a docstring
        if not func.__doc__:
            doc_params = _collect_data_for_docstring(func, annotation)
            wrapper.__doc__ = _format_docstring(doc_params)

        return wrapper

    return function_wrap
