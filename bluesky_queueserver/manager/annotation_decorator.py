# ====================================================================================
#        The contents of this file are expected to be moved to Bluesky
# ====================================================================================

import functools
import inspect
import jsonschema
import textwrap
import pprint
import re
import copy

_parameter_annotation_schema = {
    "type": "object",
    "properties": {
        "description": {"type": "string"},
        "parameters": {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "description": {"type": "string"},
                    "annotation": {"type": "string"},
                    "devices": {
                        "$ref": "#/definitions/custom_types",
                    },
                    "plans": {
                        "$ref": "#/definitions/custom_types",
                    },
                    "enums": {
                        "$ref": "#/definitions/custom_types",
                    },
                },
            },
        },
        "returns": {
            "type": "object",
            "properties": {
                "description": {"type": "string"},
            },
            "additionalProperties": {
                "annotation": {"type": "string"},
            },
        },
    },
    "additionalProperties": False,
    "definitions": {
        "custom_types": {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": [{"type": "string"}],
            },
        },
    },
}


def _print_docstring_title(title):
    """
    Print title of the docstring to a string. The title is underlined using the correct number
    of ``-`` sybmols.

    Parameters
    ----------
    title: str
        Title (such as ``Parameters``, ``Returns`` or ``Yields``).

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
            s += f" : {p_type}"
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


def _get_enclosed_str(text, prefix, suffix):
    """
    Remove prefix and suffix from the string if the string contains both prefix and suffix.
    """
    success = False
    text = text.strip()
    pattern = f"^{prefix}.+{suffix}$"
    if re.search(pattern, text):
        pattern1 = f"^{prefix}"
        pattern2 = f"{suffix}$"
        text = re.sub(pattern1, "", text)
        text = re.sub(pattern2, "", text)
        success = True
    return text, success


def _convert_annotation_to_type(param_annotation):
    """
    The purpose of this function is to convert Python parameter annotations to types
    as they are supposed to appear in docstrings. This function will need to be extended.
    (E.g. <class 'int'> should be converted to int).
    """
    enclosing_cases = (
        ("<class '", "'>"),
        ("<enum '", "'>"),
    )
    for case in enclosing_cases:
        param_annotation, success = _get_enclosed_str(param_annotation, case[0], case[1])
        if success:
            break
    return param_annotation


def _extract_yield_type(param_annotation):
    enclosing_cases = (
        (r"typing.Iterator\[", "]"),
        (r"typing.Iterable\[", "]"),
    )
    success = False
    for case in enclosing_cases:
        param_annotation, success = _get_enclosed_str(param_annotation, case[0], case[1])
        if success:
            break

    # Special case: typing.Generator(<type>, None, None) - extract <type>
    if not success:
        param_annotation, success = _get_enclosed_str(param_annotation, r"typing.Generator\[", "]")
        if success:
            p_list = param_annotation.split(",")
            if p_list:
                param_annotation = p_list[0].strip()
    return param_annotation


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
    # Description of the function
    doc_params["description"] = annotation.get("description", "")
    # Flag that tells if the function is a generator. Title for returning
    #   values for generator is 'Yields' and for regular functions it is 'Returns'
    doc_params["is_generator"] = inspect.isgeneratorfunction(func)

    doc_params["parameters"] = {}
    if parameters:  # The function may have no parameters
        # We will print names of ALL parameters from the signature
        for p_name, p in parameters.items():
            # Select description, annotation and types from available sources.
            #   Annotation (parameter of the wrapper) always overrides Python annotation.
            doc_params["parameters"][p_name] = {}

            kind = p.kind.name
            kind = kind.lower().replace("_", " ")
            doc_params["parameters"][p_name]["kind"] = kind

            desc, an, plans, devices, enums = "", "", {}, {}, {}
            if ("parameters" in annotation) and (p_name in annotation["parameters"]):
                p_an = annotation["parameters"][p_name]
                desc = p_an.get("description", "")
                if "annotation" in p_an:
                    an = p_an["annotation"]
                    # Ignore annotation if it is an empty string. Lists of plans
                    #   and devices make no sense, so don't include them.
                    if an:
                        # Now save the lists of plans and devices if any
                        plans = p_an.get("plans", {})
                        devices = p_an.get("devices", {})
                        enums = p_an.get("enums", {})

            if not an and parameters[p_name].annotation != inspect.Parameter.empty:
                an = str(parameters[p_name].annotation)

            doc_params["parameters"][p_name]["annotation"] = _convert_annotation_to_type(an)
            doc_params["parameters"][p_name]["description"] = desc
            doc_params["parameters"][p_name]["plans"] = plans
            doc_params["parameters"][p_name]["devices"] = devices
            doc_params["parameters"][p_name]["enums"] = enums

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
            desc = annotation["returns"].get("description", "")
            an = annotation["returns"].get("annotation", "")
        if not an:
            if return_annotation != inspect.Signature.empty:
                an = str(return_annotation)
    doc_params["returns"]["description"] = desc
    if doc_params["is_generator"]:
        an = _extract_yield_type(an)
    doc_params["returns"]["annotation"] = _convert_annotation_to_type(an)

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
    not_documented_str = "THE ITEM IS NOT DOCUMENTED YET ..."
    tab_size = 4

    doc = ""

    func_desc = doc_params["description"]
    if func_desc:
        doc += _print_indented_block(func_desc, indent=0, text_width=text_width)
    # The function may have no parameters
    if doc_params["parameters"]:
        doc += _print_docstring_title("Parameters")

        for p_name, p in doc_params["parameters"].items():
            doc += _print_parameter_name_and_type(p_name, p["annotation"])

            # Insert the description of the parameter
            desc = p.get("description", None)
            desc = desc if desc else not_documented_str  # desc can be None or ""
            doc += _print_indented_block(desc, indent=tab_size, text_width=text_width)

            if p["plans"]:
                doc += _print_indented_block("Allowed plans:", indent=tab_size, text_width=text_width)
                for plan_group_name, plan_list in p["plans"].items():
                    plan_list_str = ", ".join([str(_) for _ in plan_list])  # Don't use quotes
                    s = f"'{plan_group_name}': {plan_list_str}"
                    doc += _print_indented_block(s, indent=tab_size * 2, text_width=text_width)

            if p["devices"]:
                doc += _print_indented_block("Allowed devices:", indent=tab_size, text_width=text_width)
                for device_group_name, device_list in p["devices"].items():
                    device_list_str = ", ".join([str(_) for _ in device_list])  # Don't use quotes
                    s = f"'{device_group_name}': {device_list_str}"
                    doc += _print_indented_block(s, indent=tab_size * 2, text_width=text_width)

            if p["enums"]:
                doc += _print_indented_block("Allowed names:", indent=tab_size, text_width=text_width)
                for enum_group_name, enum_list in p["enums"].items():
                    enum_list_str = ", ".join([pprint.pformat(_) for _ in enum_list])  # Use quotes with 'str'
                    s = f"'{enum_group_name}': {enum_list_str}"
                    doc += _print_indented_block(s, indent=tab_size * 2, text_width=text_width)

            s = f"Kind: {p['kind']}."
            doc += _print_indented_block(s, indent=tab_size, text_width=text_width)

            if p["default"] is not None:
                s = f"Default: {p['default']}."
                doc += _print_indented_block(s, indent=tab_size, text_width=text_width)

    # Print return value type and description
    if doc_params["returns"]["annotation"] or doc_params["returns"]["description"]:
        title = "Yields" if doc_params["is_generator"] else "Returns"
        doc += _print_docstring_title(title)

        offset = 0
        s = doc_params["returns"]["annotation"]
        if s:
            doc += _print_indented_block(s, indent=0, text_width=text_width)
            offset = 1

        s = doc_params["returns"]["description"]
        if s:
            doc += _print_indented_block(s, indent=tab_size * offset, text_width=text_width)

    return doc


def parameter_annotation_decorator(annotation):
    """
    The decorator allows to attach a custom description to a function or generator function.
    If a decorated function does not have a docstring, the decorator creates a function docstring
    based on information in the dictionary passed as a parameter, function signature and standard
    parameter annotations. The purpose of the decorator is to extend the standard Python annotation
    capabilities, so the information passed to the decorator should always take precedence over
    the information that is extracted from the docstring (typically textual descriptions of
    the function, its parameters and the return values) and function signature. All the fields in
    the dictionary passed to the decorator are optional, so only the parameters that are needed
    should be specified. In many cases the complete description of the function may be generated
    from the existing docstring and parameter hints and the use of the decorator may be avoided.

    The purpose of the custom parameter annotations is provide alternative type hints for some
    parameters. Those alternative hints may be necessary for validation of plan parameters outside
    the Run Engine environment. Standard Python types, such as ``float``, ``int`` or ``str``),
    may be unambiguously specified as parameter hints and used for plan validation and typechecking
    (e.g. with ``mypy``). Some plans may accept references to Ophyd devices or other bluesky plans
    as parameters. The correct type hints for those parameters would be ``ophid.device.Device``
    for the Ophyd device reference and ``typing.Callable`` for a reference to Bluesky plan. Those
    hints are suitable for type checking, but not helpful for validation of parameters performed
    outside Run Engine environment, where the devices and plans are represented by their names
    and validation of values include verification if a given device or plan can be used as
    the value for the given parameter. The custom description for such parameter specifies a set
    of lists (groups) of names of devices (plans) that are allowed to be used, tells if those names
    represent devices, plans or simply strings (``devices``, ``plans``, ``enums``) and allows
    to specify arbitrarily complex type hints using Python ``typing`` module (names of the name groups
    are used as custom types, see the example below).

    Another potential use of the decorator is to provide shorter textual descriptions (e.g. for tooltips)
    for the parameters in case if long description extracted from the function docstring is too detailed.

    The decorator does not change the function and does not overwrite an existing docstring.
    The decorator does not generate function descriptions, instead the parameter dictionary
    passed to the decorator is saved as ``_custom_parameter_annotation_`` attribute of the function
    and may be used by later for generation of plan descriptions. The additional field
    ``_custom_parameter_annotation_["docstring_autogenerated"] = True`` is added to the dictionary
    if the function docstring was generated by the decorator (autogenerated docstring should not be
    used for analysis, instead all data should be extracted from the function signature and the parameter
    dictionary).

    The decorator verifies if the parameter dictionary matches JSON schema and if names of all
    the parameter names exist in the function signature. The exception is raised if there is
    a mismatch.

    The following is an example of custom annotation (Python dictionary) with inline comments:

    .. code-block:: python

        {
            # Optional function description. The function and parameter descriptions are formatted
            #   to fit the width of 80 characters.
            "description": "Custom annotation with plans and devices.",

            # 'parameters' block is optional. Empty 'parameters' block will also be accepted.
            # The keys in the parameter block are names of the function arguments. The docstring
            # entries will be generated based on the function argument type hints
            # if no custom annotation for the parameter is provided. The parameters listed
            # in this block must exist in function signature, otherwise an exception will be raised.
            # Function signature for this example is:
            #     func(plan_or_plans, some_name, dwell_time, str_or_int_or_float, devices)
            # The parameters may have default values, which must be specified as part of the function
            # definition. The default values are included in the generated docstring.
            "parameters": {
                # Parameter that accepts a plan (unusual, but supported by the decorator).
                "plan_or_plans": {
                    # The parameter text descriptions are optional.
                    "description": "Parameter that accepts a plan or a list of plans.",
                    "annotation": "typing.Union[Plan1, typing.List[Plan2]]",
                    "plans": {
                        # Here we have two groups of plans. Names of the plans are used as types in
                        #   'annotation'. The example of annotation above allows to pass one plan
                        #   from the group 'Plan1' or a list of plans from 'Plan2'.
                        "Plan1": ("count", "scan", "gridscan"),
                        "Plan2": ("some", "more", "plan", "names"),
                    },
                },
                "some_name": {
                    # This parameter will probably have type hint 'some_name: str`.
                    "description": "String selected from a list of strings (similar to enum)",
                    "annotation": "Names",
                    # Note, that "devices", "plans" and "names" are treated identically during type
                    #   checking, since they represent lists of strings. One parameter may have
                    #   name groups from "devices", "plans" and "enums" combined in complex expression
                    #   using 'typing' module.
                    "enums": {
                        "Names": ("name1", "name2", "name3"),
                    },
                }
                "dwell_time": {
                    "description": "Dwell time.",
                    # The simple type 'float' should probably be specified as a parameter hint, but it can
                    #   also be specified here. Putting it in both places will also work.
                    "annotation": "float",
                }
                "str_or_int_or_float": {
                    "description": "Some values that may be of 'str', 'int' or 'float' type.",
                    # The following expression can also be put as the parameter hint.
                    "annotation": "typing.Union[str, int, float]",
                }
                "devices": {
                    "description": "Parameter that accepts the list of devices.",
                    "annotation": "typing.List[Device]",
                    # Here we provide the list of devices. 'devices' and 'plans' are treated
                    #   similarly, but it may be useful to distinguish lists of plans and devices
                    #   on the stage of plan parameter validation.
                    "devices": {
                        "Device": ("det1", "det2", "det3"),
                    },
                },
            },

            # 'returns' block is optional. Returns block is used both for regular functions and
            #   generator functions. The created docstring will have 'Yields' or 'Returns' block
            #   depending on whether the function is a generator. Also if the function is
            #   a generator, the yield type will be automatically extracted from the expressions
            #     typing.Iterator[<type>]
            #     typing.Iterable[<type>]
            #     typing.Generator[<type>, sometype, sometype]
            #   and placed in docstring as Yield type.
            "returns": {
                "description": "Sequence of number",
                "annotation": "str"
            },
        }

    Raises
    ------
    jsonschema.ValidationError
        The parameter dictionary does not match the schema.
    ValueError
        The dictionary contains parameters that are not function arguments (not in the function signature).
    """

    def function_wrap(func):
        if inspect.isgeneratorfunction(func):

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return (yield from func(*args, **kwargs))

        else:

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

        # Always create the copy (annotation dictionary may be reused)
        nonlocal annotation
        annotation = copy.deepcopy(annotation)

        jsonschema.validate(
            instance=annotation, schema=_parameter_annotation_schema, types={"array": (list, tuple)}
        )

        sig = inspect.signature(func)
        parameters = sig.parameters

        param_unknown = []
        if "parameters" in annotation:
            for p in annotation["parameters"]:
                if p not in parameters:
                    param_unknown.append(p)
        if param_unknown:
            msg = (
                f"Custom annotation parameters {param_unknown} are not "
                f"in the signature of function '{func.__name__}'."
            )
            raise ValueError(msg)

        # Create a docstring from the annotation if the function does not have a docstring
        if not func.__doc__:
            doc_params = _collect_data_for_docstring(func, annotation)
            wrapper.__doc__ = _format_docstring(doc_params)
            # Mark docstring as autogenerated. Autogenerated docstring should not be
            #   used as independent source of parameter descriptions or any other data.
            annotation["docstring_autogenerated"] = True

        setattr(wrapper, "_custom_parameter_annotation_", annotation)

        return wrapper

    return function_wrap
