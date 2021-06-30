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
parameters of optional ``parameter_annotation_decorator`` that are collected and saved by
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
provides ``parameter_annotation_decorator`` (:ref:`parameter_annotation_decorator`),
which allows to override any annotation item. The decorator is optional and should be
used only when necessary.

.. _plans_without_annotation:

Plans Without Annotation
------------------------

!!! Any object that could be serialized as JSON may be passed to a plan parameter.

Parameter annotations are optional. Plans without annotation can be successfully managed
by Queue Server. The text descriptions of plans and plan parameters are not used by
Queue Server itself and do not influence the operation. Provide text descriptions if
they are displayed to users on the client side.

Parameter types and default values are used for plan validation. The parameters that
do not have a default value are **required** parameters and must be explicitly passed
to the plan. The parameters with a default value are **optional**: the default value
is used if the parameter value is not passed to the plan.

If parameter type is specified, then the parameter value passed to the plan is validated.
If validation fails, then the plan is rejected by the server. If parameter type is
not specified, validation will succeed for the value of any type (generic type ``typing.Any``
is used during the parameter validation).

It is also allowed to provide types of plan parameters for which type validation is
absolutely critical. This approach is generally not recommended. Type annotation may be
skipped for parameters that may accept values of types that are not supported by
Queue Server in plans that are also used without Queue Server, e.g. started directly
from IPython (type annotations are not used by Python, but specifying incorrect types
just to satisfy Queue Server requirements may create misleading code).

The examples of the plans without annotation:

.. code-block:: python

  def plan_demo1a(npts, delay):
      # Parameters 'npts' and 'delay' will accept values of any type.
      #   No validation of parameter value types will be performed
      #   before execution of the plan is started. The plan will probably
      #   fail if parameters are of incorrect type.
      <code implementing the plan>

  def plan_demo1b(npts, delay=1.0):
      # Same as 'plan_demo1' except the default value for parameter 'delay' is
      #   specified, which makes the parameter 'delay' optional.
      #   No type validation is performed.
      <code implementing the plan>

Queue Server supports plans that are accepting references to devices and other plans
that are defined in startup scripts and exist in RE Worker namespace. When submitting
a plan that has a parameter accepting a reference to a device or a plan, the device
or plan must be represented by its name (type ``str``). The name is then replaced by
reference to the existing device or plan before the parameters are passed to the plan
for execution. If the submitted value is a list, tuple or dictionary, the algorithm
iteratively searches the tree formed by list and tuple elements and dictionary values
to detect strings that represent device or plan names and replace those strings
with device and plan references.

The operation of replacing plan and device names with references to objects from RE Worker
namespace is performed for each parameter, which has no type annotation. In this case
every string that matches the name of a device or a plan from the lists of allowed
devices and plans for the user submitting the plan is replaced by the reference to the
respective object from RE Worker namespace.

Let's consider an example of a plan with parameter ``detectors`` that is expected to
receiving a list of detectors:

.. code-block:: python

  from ophyd.sim import det1, det2, det3
  # Assume that the detectors 'det1', 'det2', 'det3' are in the list
  #   of allowed devices for the user submitting the plan.

  def plan_demo1c(detectors, npts):
      # The parameter 'detectors' is expected to receive a list of detectors.
      # The parameter type is not specified, so the type is not validated.
      <code implementing the plan>

If the value of ``detectors`` in the submitted plan is ``"detectors": ["det1", "det3"]``,
then the strings ``"det1"`` and ``"det3"`` are replaced with references to objects
``det1`` and ``det3`` and the plan is executed as if it was called from IPython
using the following command:

.. code-block:: python

  RE(plan_demo1c([det1, det3], <value of npts>))

Blindly attempting to convert all strings in the passed parameter to references
works well in most of the simple cases. So plans without type annotations can be used
as long as there is no need to pass strings that match names of plans and devices in
the workspace. There are two issues with this approach:

* A plan accepts the name of a plan or device (not reference to the object):

  .. code-block:: python

    from ophyd.sim import det1, det2, det3
    # Assume that the detectors 'det1', 'det2', 'det3' are in the list
    #   of allowed devices for the user submitting the plan.

    def plan_demo1d(detector_names, npts):
        # The parameter 'detector_names' is expected to receive a list of detector names.
        <code implementing the plan>

    def plan_demo1e(detector_names: typing.List[str], npts):
        # The parameter 'detector_names' is expected to receive a list of detector names.
        <code implementing the plan>

  If the value ``"detector_names": ["det1", "det3"]`` is passed to the plan ``plan_demo1d`,
  then the detector names are converted to references, which is not desirable. Annotating
  the type of parameter ``detector_names`` (see ``plan_demo1e``) will prevent names from
  being converted to references. This also enables type validation for parameter
  ``detector_names`` and the plan is going to be rejected by Queue Server if the value
  is not a list of strings.

* The name of the device or plan that is passed to the plan is not a name of an existing object
  or is not in the list of allowed plans and devices. For example, assume that
  ``"detectors": ["det1", "det4"]`` is passed to ``plan_demo1c``. The detector ``det4``
  is not present in RE Worker namespace, so it will not be converted to reference. As a result,
  the ``detectors`` parameter will receive a list ``[det1, "det4"]`` and the plan will fail
  during execution. In many cases this behavior is not desirable and it is preferred that
  the plan is rejected by Queue Server at the time when it is submitted.
  ``parameter_annotation_decorator`` (:ref:`parameter_annotation_decorator`) allows
  to define custom lists of devices and plans that could be accepted by any parameter
  of the plan:

  .. code-block:: python

    from ophyd.sim import det1, det2, det3
    # Assume that the detectors 'det1', 'det2', 'det3' are in the list
    #   of allowed devices for the user submitting the plan.

    from bluesky_queueserver import parameter_annotation_decorator

    @parameter_annotation_decorator({
        "parameters": {
            "detectors": {
                "annotation": "typing.List[DevicesType1]",
                "devices": {"DevicesType1": ["det1", "det2", "det3"]}
            }
        }
    })
    def plan_demo1f(detectors, npts):
        # The parameter 'detector_names' is expected to receive a list of detector names.
        <code implementing the plan>

  The type annotation in the decorator overrides the type annotation in the function header.
  Type validation is now enabled, so only the lists of device names from predefined set will now be
  accepted by the server. If the submitted plan contains ``"detectors": ["det1", "det4"]``,
  the plan is rejected.

.. note::

  Value of any type that can be serialized as JSON can be passed to the plan if
  the respective parameter type is not defined or defined as ``typing.Any``.
  In the latter case the strings are not going to be substituted with references.


.. _supported_types:

Supported Types
---------------

There are restrictions on types used for type annotations and default values. If a plan header
contains parameter type annotation that is not supported, Queue Server ignores the annotation
and the plan is processed as if the parameter contains no annotation. If unsupported type annotation
is defined in ``parameter_annotation_decorator``, then processing of the plan fails (startup files
can not be loaded). If the default value defined in plan header or in the decorator has unsupported
type, then processing of the plan fails.

.. note::

  Type annotations and default values defined in ``parameter_annotation_decorator`` override type
  annotations and default values defined in plan header. If type or default value is defined
  in the decorator, the respective type and default value from the header is not analyzed
  by the Queue Server. If the plan must have unsupported type annotation or default value
  defined in the header, then the ``parameter_annotation_decorator`` should override
  the type or default value in order for the plan processing to work.

**Supported types for type annotations.** Type annotations may be native Python types
(such as ``int``, ``float``, ``str``, etc.), ``NoneType``, or generic types that are based
on native Python types (``typing.List[typing.Union[int, str]]``). Generally, the
operation of creating the type object from its string representation using ``eval`` function
and the namespace with imported ``typing`` module and ``NoneType`` type should be successful.

**Supported types of default values.** The default values can be objects of native Python
types and literal expressions with objects of native Python types. The default value should
be reconstructable with function ``ast.literal_eval``, i.e. for the default value ``vdefault``,
the operation ``ast.literal_eval(f"{vdefault!r}")`` should complete successfully.

The following is an example of a plan with type annotation that will not be accepted
by Queue Server. Since the type annotation is defined in the plan header, it is ignored
and the plan is treated as if the parameter ``detector`` has no type annotation.

.. code-block:: python

  from ophyd import Device

  def plan_demo2a(detector: Device, npts=10):
      # Type 'Device' is not recognized by Queue Server, because it needs to be imported
      #   from Ophyd. Type annotation will be ignored by Queue Server. Use
      #   'parameter_annotation_decorator' to override annotation for 'detector' of
      #   type annotation is required.
      <code implementing the plan>

The parameter ``detector`` has the default value of invalid type and loading of the startup
script containing the plan will fail. The issue can be fixed by overriding the default value
for the parameter ``detector`` using ``parameter_annotation_decorator``
(:ref:`parameter_annotation_decorator`).

.. code-block:: python

  from ophyd.sim import det1

  def plan_demo2b(detector=det1, npts=10):
      # Default value 'det1' can not be used with the Queue Server.
      #   Fix: use 'parameter annotation decorator to override the default value.
     <code implementing the plan>


Defining Types in Plan Header
-----------------------------

When loading startup scripts, Queue Server is collecting plans from the namespace and analyzing
plan signatures. If a plan signature contains type hints, the hints are validated, and their string
representation is saved. Hints that fail validation are ignored and Queue Server treats those
parameters as having no type annotation.

.. note::
  Queue Server ignores type hints defined in plan signature for parameters that have
  type annotations defined in ``parameter_annotation_decorator``.

The acceptable types include Python base types, ``NoneType`` and imports from ``typing`` module.
Following are the examples of plans with type hints.

.. code-block:: python

  import typing
  from typing import List, Optional

  def plan_demo3a(detector, name: str, npts: int, delay: float=1.0):
      # Type of 'detector' not defined, therefore Queue Server will find and attempt to
      #   replace all strings passed to this parameter by references to objects in
      #   RE Worker namespace. Specifying a type hint for the ``detector`` parameter
      #   will disable the automatic conversion. To enable conversion use
      #   'parameter_annotation_decorator'.
      <code implementing the plan>

  def plan_demo3b(positions: typing.Union[typing.List[float], None]=None):
      # Explicitly using the 'typing' module. Setting default value to 'None'
      <code implementing the plan>

  def plan_demo3c(positions: Optional[List[float]]=None):
      # This example is precisely identical to the previous example. Both hints are
      #   converted to 'typing.Union[typing.List[float], NoneType]' and
      #   correctly processed by the Queue Server.
      <code implementing the plan>


Defining Default Values in Plan Header
--------------------------------------

Follow Python syntax rules to defining default values of parameters. The type of
the default value must be supported by the Queue Server (see :ref:`supported_types`).
If the default value must have type, which is not supported, override the default
value in ``parameter_annotation_decorator``.

.. note::

  If the default value is defined in the ``parameter_annotation_decorator``,
  Queue Server ignores the default value defined in the header. The default value
  in the header is required if there is a default value defined in the decorator.


Parameter Descriptions in Docstring
-----------------------------------

Queue Server will extract text descriptions of the plan and parameters from NumPy-style
docstrings. Type information specified in docstrings is ignored. The example below
shows a plan with the docstring that is successfully processed by Queue Server.
Plan and parameter descriptions in the docstring contain some notes on the format.

.. code-block:: python

  def plan_demo4a(detector, name, npts, delay=1.0):
      """
      This is the description of the plan that could be passed
      to the client and displayed to users.

      Parameters
      ----------
      detector : ophyd.Device
          The detector (Ophyd device). Space is REQUIRED before
          and after ':' that separates the parameter name and
          type.
      name
          Name of the experiment. Type is optional. Queue Server
          will still successfully process the docstring.
          Documenting types of all parameters is recommended
          practice.
      npts : int
          Number of experimental points.
      delay : float
          Dwell time.
      """
      <code implementing the plan>


.. _parameter_annotation_decorator:

Parameter Annotation Decorator
------------------------------

The ``parameter_annotation_decorator`` allows to overwrite any annotation item of the plan,
including text descriptions of the plan and parameters, parameter type annotations and
default values. The decorator can be used to define all annotation items of a plan, but
it is generally advised that its use is limited only to cases when absolutely necessary.

.. note::

  If the default value of a parameter is defined in the decorator, the parameter must
  have a default value defined in the header. The default values in the decorator and
  the header do not have to match. See the :ref:`notes <default_values_in_decorator>`
  for the use case.

Plan and Parameter Description
++++++++++++++++++++++++++++++

Text descriptions of plans and parameters are not used by Queue Server, so any text
descriptions defined in docstrings will be accepted. In some applications it may be
more appropriate to have different versions of descriptions in plan documentation
(e.g. technical description) and displayed to users (e.g. instructions on how
to use plans remotely). The decorator allows to override plan and/or parameter
descriptions displayed to the user.

All parameters in `parameter_annotation_decorator` are optional. In the following
example, the description of parameter `npts` from the docstring is not overridden
in the decorator.

.. code-block::  python

  from bluesky_queueserver import parameter_annotation_decorator

  @parameter_annotation_decorator({
      "description": "Plan description displayed to users.",
      "parameters": {
          "detector": {
              "description":
                  "Description of the parameter 'detector'\n" \
                   "displayed to Queue Server users",

          }
          "name": {
              "description":
                  "Description of the parameter 'name'\n" \
                  "displayed to Queue Server users",
          }
      }
  })
  def plan_demo4a(detector, name, npts):
      """
      Plan description, which is part of documentation and not
      visible to Queue Server users.

      Parameters
      ----------
      detector : ophyd.Device
          The detector. Technical description not visible to
          Queue Server users.
      name
          Name of the experiment. Technical description not
          visible to Queue Server users.
      npts : int
          Number of experimental points. Description remains
          visible to Queue Server users, because it is not overridden
          in the decorator.
      """
      <code implementing the plan>


Parameter Types
+++++++++++++++

Parameter type hints defined in the plan header can be overridden in
``parameter_annotation_decorator``. The type annotations defined in the decorator
do not influence execution of plans in Python. Overriding types should be avoided
whenever possible.

.. note::

  String representation of types should be used in the decorator. E.g. ``"str"``
  will represent string type, ``"typing.List[int]"`` will represent the array
  of integers etc. Module name ``typing`` is required when defining generic types.

The option of defining type annotations can be used to override hide type hints
that are not supported by Queue Server. But the main reason behind this option is
the ability to define types using custom enums with names of plans and devices.
Names of plans and devices that are included in custom enums are converted
to references to plans and devices before execution of the plan. The feature also
allows to define custom enums with predefined sets of string values, which are
not converted to references. Custom enums are used for validation of parameter
values. The sets of names or strings may also be used on the client side to
generate widgets (e.g. populate combo boxes for selecting values).

.. code-block:: python

  from typing import List
  from ophyd import Device
  from ophyd.sim import det1, det2, det3, det4, det5
  from bluesky_queueserver import parameter_annotation_decorator

  @parameter_annotation_decorator({
      "parameters": {
          "detector": {
              # 'DetectorType1' is the type name (should be a valid Python name)
              "annotation": "DetectorType1",
              # 'DetectorType1' is defined as enum with string values
              #   'det1', 'det2' and 'det3'
              "devices": {"DetectorType1": ["det1", "det2", "det3"]},
          }
      }
  })
  def plan_demo5a(detector, npts: int, delay: float=1.0):
      # Type hint for the parameter 'detector' in the header is not required.
      # Queue Server will accept the plan if 'detector' parameter value is
      #   a string with values 'det1', 'det2' or 'det3'. The string will
      #   be replaced with the respective reference before the plan is executed.
      #   Plan validation will fail if the parameter value is not in the set.
      <code implementing the plan>

  @parameter_annotation_decorator({
      "parameters": {
          "detectors": {
              # Note that type definition is a string !!!
              # Type names 'DetectorType1' and 'DetectorType2' are defined
              #   only for this parameter. The names may be defined differently
              #   for the other parameters of the plan if necessary, but doing
              #   so is not recommended.
              "annotation": "typing.Union[typing.List[DetectorType1]" \
                            "typing.List[DetectorType2]]",
              "devices": {"DetectorType1": ["det1", "det2", "det3"],
                          "DetectorType2": ["det1", "det4", "det5"]},
          }
      }
  })
  def plan_demo5b(detectors: List[Device], npts: int, delay: float=1.0):
      # Type hint contains correct Python type that will be passed to the parameter
      #   before execution.
      # Queue Server will accept the plan if 'detectors' is a list of strings
      #   from any of the two sets. E.g. ['det1', 'det3'] or ['det4', 'det5']
      #   will be accepted and ['det2', 'det4'] will be rejected (because the
      #   detectors are from different sets.
      <code implementing the plan>

Similar syntax may be used to define custom enums for plans (use ``"plans"`` dictionary key
instead of ``"devices"``) or strings (use ``"enums"`` dictionary key). The strings listed
as ``"devices"`` are converted to references to devices and the strings listed as ``"plans"``
are converted to references to plans before plan execution. Strings listed under ``"enums"``
are not converted to references, but the plan parameter validation is performed the same way.
Mixing devices, plans and enums for one parameter is possible (Queue Server will handle
the types correctly), but not recommended.

The decorator supports three built-in types: ``AllDetectors``, ``AllMotors`` and ``AllFlyers``.
The types should not need to be defined in the parameter annotation. When those types are
used, all detectors (readable devices), all motors (readable and writable devices) or
all flyers (flyable devices) from the namespace will be automatically included in the list.
Explicitly defining those types in the annotation for a parameter overrides the default
behavior for this parameter.

.. code-block:: python

  from ophyd.sim import det1, det2, det3, det4
  from bluesky_queueserver import parameter_annotation_decorator

  @parameter_annotation_decorator({
      "parameters": {
          "detectors": {
              # 'AllDetectors' is the built-in type. All detectors (readable
              #   devices) from the namespace will be automatically included
              #   in the list ('det1', 'det2', 'det3' and 'det4').
              "annotation": "typing.List[AllDetectors]",
              # Explicitly defining the type 'AllDetectors' will override
              #   the default behavior (only for this parameter).
          }
      }
  })
  def plan_demo5c(detectors, npts: int, delay: float=1.0):
      <code implementing the plan>

Definitions of custom enums for devices or plans may include any devices from RE Worker
namespace. The definitions are included in descriptions of plans in the list of existing
plans. When lists of allowed plans are generated for user groups, type definitions are
filtered based on user group permissions, so that only the devices and plans that are
allowed for the user group remain. This allows to use entries from downloaded lists of
allowed plans for validation of plans and for generation of user interfaces without
verification user permissions, since it is guaranteed, that the type definitions
will contain only devices and plans that the current user is allowed to use.
Filtering of the type definitions may lead to some lists to become empty in case
current user does not have rights to use any of the devices or plans in the original
type definition.

.. _default_values_in_decorator:

Default Values
++++++++++++++

Overriding default values defined in plan header with different values define in the decorator
is possible, but generally not recommended unless absolutely necessary. Overriding the default
value may be justified in cases when the default value defined in the header can not be
passed to the plan via Queue Server and the default value defined in the decorator has
supported type, but leads to the same result when passed to the plan.

.. note::

  The default value defined in the decorator must be represented as a string. E.g. ``"10"``
  should be used for integer value ``10``, ``"'det1'"`` represents the string value ``'det1'``
  and ``"['det1', 'det2']"`` should be used to represent an array of strings.

The following example illustrates the user case which requires overriding the default value.
In this example, the default value for the parameter ``detector`` is a reference to ``det1``,
which has unsupported type. It is required that when the plan is submitted to Queue Server,
and no value for ``detector`` parameter is provided, then the string ``"det1"`` is used
as a default value, which is then substituted by reference to ``det1``. The decorator
contains the definition of custom enum for supported device names and sets the default
value as a string representing the name of one of the supported devices.

.. code-block:: python

  from ophyd.sim import det1, det2, det3
  from bluesky_queueserver import parameter_annotation_decorator

  @parameter_annotation_decorator({
      "parameters": {
          "detector": {
              "annotation": "DetectorType1",
              "devices": {"DetectorType1": ["det1", "det2", "det3"]},
              "default": "'det1'",
          }
      }
  })
  def plan_demo6a(detector=det1, npts: int, delay: float=1.0):
      # The default value for the parameter 'detector' is reference to 'det1'
      #   when the plan is started from IPython. If the plan is submitted to
      #   the queue and no value is provided for the parameter 'detector', then
      #   it is going to be set to the string value '"det1"', which will be
      #   substituted by the reference to the detector 'det1' before the plan
      #   is set for executed.
      <code implementing the plan>



.. _plan_annotation_api:

Plan Annotation API
-------------------

.. autosummary::
    :nosignatures:
    :toctree: generated

    parameter_annotation_decorator

