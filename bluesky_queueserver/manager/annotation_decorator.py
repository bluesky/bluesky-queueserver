# ====================================================================================
#        The contents of this file are expected to be moved to Bluesky
# ====================================================================================

import functools
import inspect
import jsonschema

_parameter_annotation_schema = {
    "type": "object",
    "required": ["description", "parameters"],
    "properties": {
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
        "description": {"type": "string"},
    },
    "additionalProperties": {
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


def parameter_annotation_decorator(annotation):
    def function_wrap(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        jsonschema.validate(instance=annotation, schema=_parameter_annotation_schema)

        sig = inspect.signature(func)
        parameters = sig.parameters
        return_annotation = sig.return_annotation

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

        return wrapper

    return function_wrap
