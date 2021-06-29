========================
Annotating Bluesky Plans
========================

.. currentmodule:: bluesky_queueserver

Introduction
------------

Why do we need to annotate parameters: descriptions (text), types, default values,
providing sets for enums (for devices, plans and enums) that can be used in generation
of plan forms.

Default behavior (if parameters are not annotated). There is no requirement to annotate
every parameter, but it is recommended to do so.

Tools: function header for types and defaults, docstrings for descriptions,
decorator allows to override type, default or description for every parameter.
Decorator should be used sparingly, but it is absolutely necessary in some situations.

Supported types.

Supported default values.

Decorator allows to define sets of plan names, device names or strings and generic types
based on those sets. The parameters values are validated to make sure they satisfy the
requirements. There are built-in types that allow to include all detectors, all motors
or all flyers from the list of existing devices.

Defining Types in Plan Header
-----------------------------



Defining Default Values in Plan Header
--------------------------------------

Parameter Descriptions in Docstring
-----------------------------------

Parameter Annotation Decorator
------------------------------

Plan Annotation API
-------------------

.. _plan_annotation_api:

.. autosummary::
    :nosignatures:
    :toctree: generated

    parameter_annotation_decorator

