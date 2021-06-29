========================
Annotating Bluesky Plans
========================

.. currentmodule:: bluesky_queueserver

Introduction
------------

For each plan defined in startup script, Queue Server collects data that describes
the plan. The data is stored in the file *'existing_plans_and_devices.yaml'*. This
data is used for validation of parameters of submitted plans (:ref:`plan_validation`).
The data can also be downloaded by the client (:ref:`method_plans_allowed` 0MQ API)
and used for generating of user interfaces (e.g. GUI forms for editing plan parameters).
In this manual, the manually entered elements of a plan header, docstring and
parameters of optional `parameter_annotation_decorator` that are collected and saved by
the queue server and distributed to clients are referred as **plan annotation**.

Plan annotations are optional: Queue Server is capable of managing and executing
plans without annotations (:ref:`plans_without_annotation`). Annotating plans is
necessary if application requires validating plan parameters before plans are added
to the queue or clients need plan and parameter descriptions, types and default values
(e.g. for interacting with the user).

.. note::

  The data collected from plan annotations is sufficient to validate plan parameters
  without access to RE Worker environment or loading startup scripts. Parameter
  validation is performed whenever a new or modified plan is submitted to the Queue
  Server. Validation can be also performed on the client side by loading the lists
  of allowed plans and devices  (:ref:`method_plans_allowed` and
  `:ref:`method_devices_allowed` 0MQ API) and calling `validate_plan()`
  (:ref:`plan_validation_api`).

The following parameters may be specified as part of plan annotation:

* **Description of the plan**: arbitrary multiline text that describes the plan.
  The description may be displayed to the user for reference.

* **Descriptions for each parameter of the plan**. Similarly to a plan description,
  parameter description is a multiline text that describes the parameter and may
  be displayed to the user for reference.

* **Specification of a type for each parameter**. Parameter types are used
  for validation of plan parameters. The types may also be used in generating user
  interfaces at the client side.

* **Specification of default value for each parameter**. Specifying default value
  makes the parameter optional (following Python rules). The default values are used
  for parameter validation. They can be used for generation of user interfaces at
  the client side.

All the items in the parameter annotation are optional. Queue Server is able to load
and manage plans that are not annotated. The default values that are specified in
function header are used during plan execution, so regular Python guidelines should
be used for setting the default values.

The items for plan annotations are defined in the plan header (parameter types and
default values) and the docstring (parameter descriptions). In addition, Queue Server
provides `parameter_annotation_decorator` (:ref:`parameter_annotation_decorator`),
which allows to override any annotation item. The decorator is optional and should be
used only when necessary.

.. _plans_without_annotation:

Plans Without Annotation
------------------------

Default behavior (if parameters are not annotated). There is no requirement to annotate
every parameter, but it is recommended to do so.


Defining Types in Plan Header
-----------------------------

Supported types.


Defining Default Values in Plan Header
--------------------------------------

Supported default values.


Parameter Descriptions in Docstring
-----------------------------------

.. _parameter_annotation_decorator:

Parameter Annotation Decorator
------------------------------

Decorator allows to define sets of plan names, device names or strings and generic types
based on those sets. The parameters values are validated to make sure they satisfy the
requirements. There are built-in types that allow to include all detectors, all motors
or all flyers from the list of existing devices.


.. _plan_annotation_api:

Plan Annotation API
-------------------

.. autosummary::
    :nosignatures:
    :toctree: generated

    parameter_annotation_decorator

