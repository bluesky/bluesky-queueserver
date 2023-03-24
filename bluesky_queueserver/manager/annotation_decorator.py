import copy
import functools
import inspect

import jsonschema

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
                        "$ref": "#/definitions/custom_types_plans_devices",
                    },
                    "plans": {
                        "$ref": "#/definitions/custom_types_plans_devices",
                    },
                    "enums": {
                        "$ref": "#/definitions/custom_types",
                    },
                    "default": {},  # Expression of any time should be accepted
                    "min": {"type": "number"},
                    "max": {"type": "number"},
                    "step": {"type": "number"},
                    "convert_plan_names": {"type": "boolean"},
                    "convert_device_names": {"type": "boolean"},
                },
            },
        },
    },
    "additionalProperties": False,
    "definitions": {
        "custom_types": {
            "type": "object",
            "additionalProperties": False,
            "patternProperties": {
                "^[_a-zA-Z][_a-zA-Z0-9]*$": {
                    "type": "array",
                    "items": {"type": "string"},
                },
            },
        },
        "custom_types_plans_devices": {
            "type": "object",
            "additionalProperties": False,
            "patternProperties": {
                "^[_a-zA-Z][_a-zA-Z0-9]*$": {
                    "anyOf": [
                        {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    ],
                },
            },
        },
    },
}


def parameter_annotation_decorator(annotation):
    """
    The decorator allows to attach a custom description to a function or generator function.
    The purpose of the decorator is to extend the standard Python annotation
    capabilities. The annotation elements defined in the decorator always override
    the identical elements defined in the function header (type hints and default values) and
    the docstring (text descriptions of the function and its parameters). All the fields in
    the dictionary passed to the decorator are optional, so only the parameters that are needed
    should be specified. In many cases the complete description of the function may be generated
    from the existing docstring and parameter hints. In those cases using the decorator may be avoided.

    The purpose of the custom parameter annotations is to provide alternative type hints for some
    parameters. Those alternative hints may be necessary for validation of plan parameters outside
    the Run Engine environment. Standard Python types, such as ``float``, ``int`` or ``str``),
    may be unambiguously specified as parameter hints and used for plan validation and typechecking
    (e.g. with ``mypy``). Some plans may accept references to devices or other Bluesky plans
    defined in RE Worker namespace as parameters. Plans and devices are referred by their names
    in parameters of the plans submitted to the queue. Type validation for those parameters
    must include checking the parameter type (e.g. if the parameter is a list) and verifying
    that the values are strings from specified set (e.g. that strings in the list are the devices
    from specified set). This functionality may be achieved by defining custom enum types that
    are supported by the decorator. Custom enum types allow define list of names for ``devices``
    (names of devices from RE Worker namespace), ``plans`` (names of plans from RE Worker namespace)
    and ``enums`` (string literals). Type annotations in the decorator may use custom enum types
    to define complex types (using generic types based on ``typing`` module).

    The decorator allows to define additional parameter annotation items: ``min``, ``max`` and
    ``step``. Those items are applied only to numbers in data structures passed as parameter values.
    If ``min`` or ``max`` values are defined for a parameter, each number found
    in the data structure (such as list, tuple, dictionary, list of dictionaries etc.)
    is compared to the values of ``min`` and ``max`` and the plan is rejected if any value
    is out of range. Range validation is inteded primarily for using with parameters that
    accept numbers or lists/tuples of numbers. The ``step`` value specified in annotation
    is not used by Queue Server, but passed to the client application and may be useful for
    generating GUI components (e.g. spin boxes).

    Another potential use of the decorator is to provide shorter text descriptions (e.g. for tooltips)
    for the parameters in case long description extracted from the function docstring is too detailed.
    The plan and parameter descriptions defined in the decorator override the descriptions from
    the docstring, hiding them from the client applications.

    The decorator does not change the function and does not overwrite an existing docstring.
    The decorator does not generate function descriptions, instead the parameter dictionary
    passed to the decorator is saved as ``_custom_parameter_annotation_`` attribute of the function
    and may be used later for generation of plan descriptions.

    The decorator verifies if the parameter dictionary matches JSON schema and if names of all
    the parameter names exist in the function signature. The exception is raised if there is
    a mismatch.

    The following is an example of custom annotation (Python dictionary) with inline comments.
    The example is not practical. It demonstrates syntax of defining various annotation elements
    using the decorator:

    .. code-block:: python

        from bluesky_queueserver import parameter_annotation_decorator

        @parameter_annotation_decorator({
            # Optional function description.
            "description": "Custom annotation with plans and devices.",

            # 'parameters' dictionary is optional. Empty 'parameters' dictionary will
            #   also be accepted. The keys in the parameter dictionary are names of
            #   the function parameters. The parameters must exist in function signature,
            #   otherwise an exception will be raised.
            "parameters": {

                "experiment_name": {
                    # This parameter will probably have type hint 'some_name: str`.
                    "description": "String selected from a list of strings (similar to enum)",
                    "annotation": "Names",

                    # Note, that "devices", "plans" and "names" are treated identically
                    #   during type checking, since they represent lists of strings.
                    #   One parameter may have name groups from "devices", "plans"
                    #   and "enums" combined in complex expression using 'typing'
                    #   module. The names listed as ``enums`` are not replaced by
                    #   references to objects in RE Worker namespace.
                    "enums": {
                        "Names": ("name1", "name2", "name3"),
                    },
                }

                "devices": {
                    "description": "Parameter that accepts the list of devices.",
                    "annotation": "typing.List[typing.Union[DeviceType1, DeviceType2]]",

                    # Here we provide the list of devices. 'devices' and 'plans' are
                    #    treated similarly, but should be separated into different sections
                    #    to implement proper parameter validation. The devices may be listed
                    #    explicitly by name or using patterns based on regular expressions.
                    #    The list may contain any combination of names and patterns.
                    #    The patterns are distinguished from names by leading ``:`` at the
                    #    beginning of each pattern. Patterns may contain multiple regular
                    #    expressions separated with ``:``: the first expression is applied
                    #    to the device name, the second expression to subdevices of the
                    #    device, the third - to subdevices of subdevices etc. For example,
                    #    the pattern ``:^stage_:^det:val$ would match the devices with names
                    #    such as ``stage_sim.det2.val``.
                    #
                    #    Adding ``-`` before an expression (after ``:``) excludes the matching
                    #    devices/subdevices from the list (e.g. ``:^stage_:-^det:val$ adds
                    #    ``stage_sim`` and ``stage_sim.det2.val`` to the list, but not
                    #    ``stage_sim.det2``).
                    #
                    #    A regular expression may be applied to the full name (including
                    #    subdevice names) or remaining name of the device. An expression
                    #    could be labelled as 'full-name' by placing ``?`` before it.
                    #    For example ``:?^stage_.*val$`` would pick all devices with names
                    #    starting with ``stage_`` and ending with ``val`` from the complete
                    #    tree of existing devices. The search depth may be restricted by
                    #    adding ``depth`` parameter (e.g. ``:?^stage_.*val$:depth=5``
                    #    restricts the search depth to 5).
                    #
                    #    The both types of expressions could be mixed in one pattern.
                    #    The full-name expression must be the last in the pattern and
                    #    is applied to the remaining part of the name. For example
                    #    ``:^stage_:?^det.*val$:depth=4`` selects the device ``stage_sim``
                    #    and all its subdevices starting with ``det`` and ending with ``val``
                    #    up to the total depth of 5 (depth=4 is set for the full-name expression).
                    #    Note, that searching devices using full-name expressions is less
                    #    efficient and should be minimized or avoided if possible.
                    #
                    #    Regular expressions may be preceded with one the keywords that
                    #    defines the type of matching devices: ``__READABLE__``,
                    #    ``__MOTOR__``, ``__DETECTOR__`` and ``__FLYABLE__``.
                    #    For example, ``__READABLE__:^stage_:?^det.*val$:depth=4``
                    #    selects only the devices that are readable.
                    "devices": {
                        "DeviceType1": ("det1", ":^det2$", ":^det:val$", ":?^det.*val$"),
                        "DeviceType2": ("__MOTOR__:?.*:depth=5"),
                    },
                    # Set default value to string 'det1'. The default value MUST be
                    #   defined in the function header for the parameter that has
                    #   the default value overridden in the decorator. The default
                    #   value in the header is reference to 'det1'.
                    "default": "'det1'",
                },

                # Parameter that accepts a plan or a list of plans
                #   (plans must exist in RE Worker namespace,
                #    reference to the plans will be passed to the plan).
                "plans_to_run": {
                    # Text descriptions of parameters are optional.
                    "description": "Parameter that accepts a plan or a list of plans.",
                    "annotation": "typing.Union[PlanType1, typing.List[PlanType2]]",

                    # Similarly to lists of name patterns for devices, the lists of
                    #   patterns for plan may include device names and name patterns
                    #   based on regular expressions. The patterns always start with
                    #   ``:`` and may contain only a single regular expressions.
                    #   The regular expression may be preced with ``+``, ``-`` and ``?``
                    #   characters (e.g. ``:?^count$), but those characters are ignored
                    #   by the processing algorithm. ``depth`` can not be used in
                    #   the plan patterns.
                    #
                    # In the following example, two lists of plan names are defined.
                    #   Names of the plan lists are used as types in 'annotation'.
                    #   The example of annotation above allows to pass one plan from
                    #   the group 'PlanType1' or a list of plans from 'PlanType2'.
                    "plans": {
                        "PlanType1": (":^count$", "scan", "gridscan"),
                        "PlanType2": ("more", "plan", "names", ":^move_"),
                    },
                },

                "dwell_time": {
                    "description": "Dwell time.",
                    # The simple type 'float' should probably be specified as
                    #   a parameter hint, but it can also be specified here. Putting
                    #   it in both places will also work.
                    "annotation": "float",
                    "min": 0.1,
                    "max": 10.0,
                    "step": 0.1,
                }

                "str_or_int_or_float": {
                    "description": "Some values that may be of 'str', 'int' or 'float' type.",
                    # The following expression can also be put as the parameter hint.
                    "annotation": "typing.Union[str, int, float]",
                }

            },
        })
        def plan(experiment_name, plans_to_run, devices=det1, dwell_time=1.0, str_or_int_or_float=5):
            <code implementing the plan>

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

        # Create custom validator which recognizes lists and tuples as jsonschema 'arrays'
        def is_array(checker, instance):
            return isinstance(instance, (list, tuple))

        type_checker = jsonschema.Draft3Validator.TYPE_CHECKER.redefine("array", is_array)
        ValidatorCustom = jsonschema.validators.extend(jsonschema.Draft7Validator, type_checker=type_checker)
        validator = ValidatorCustom(schema=_parameter_annotation_schema)

        # Validate annotation
        validator.validate(annotation)

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

        setattr(wrapper, "_custom_parameter_annotation_", annotation)

        return wrapper

    return function_wrap
