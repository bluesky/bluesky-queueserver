import re


def filter_plan_descriptions(plans_source):
    """
    Filter the descriptions of the allowed plan to make them easier to use by the web client.
    There are some assumptions that are made when reducing complex types that are used
    for parameter validation by the RE Manager to simple types. Those assumptions may not
    always be valid, therefore operation of the function should always be validated when
    working with the plans that require complex types.

    Parameters
    ----------
    plan_source : dict
        Dictionary of plans: key - plan name, value - plan parameters.
    """

    plans_filtered = {}
    for p_name, p_items in plans_source.items():

        plan = dict()
        # Plan name - mandatory (actually it is always equals 'pname')
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
