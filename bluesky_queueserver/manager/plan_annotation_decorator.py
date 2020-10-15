# ====================================================================================
#        The contents of this file are expected to be moved to Bluesky
# ====================================================================================

import functools


def parameter_annotation_decorator(annotation):
    def function_wrap(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        param_unknown = []
        for p in annotation:
            if p not in func.__code__.co_varnames:
                param_unknown.append(p)
        if param_unknown:
            msg = f"Annotation parameters {param_unknown} are not parameters of the function '{func.__name__}'."
            raise ValueError(msg)
        setattr(wrapper, "__custom_parameter_annotation__", annotation)

        return wrapper

    return function_wrap
