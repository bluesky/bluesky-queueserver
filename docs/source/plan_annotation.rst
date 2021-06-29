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

  def plan_demo1(npts, delay):
      # Parameters 'npts' and 'delay' will accept values of any type.
      #   No validation of parameter value types will be performed
      #   before execution of the plan is started. The plan will probably
      #   fail if parameters are of incorrect type.
      <code implementing the plan>

  def plan_demo2(npts, delay=1.0):
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
iteratively search the tree formed by list and tuple elements and dictionary values
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

  def plan_demo3(detectors, npts):
      # The parameter 'detectors' is expected to receive a list of detectors.
      # The parameter type is not specified, so the type is not validated.
      <code implementing the plan>

If the value of ``detectors`` in the submitted plan is ``"detectors": ["det1", "det3"]``,
then the strings ``"det1"`` and ``"det3"`` are replaced with references to objects
``det1`` and ``det3`` and the plan is executed as

.. code-block:: python

  RE(plan_demo3([det1, det3], <value of npts>))

Blindly attemtping to converting all strings in the passed parameter to references
works well in most of the simple cases. So plans without type annotations can be used
as long as there is no need to pass strings that match names of plans and devices in
the workspace. There are two issues with this approach:

* A plan accepts the name of a plan or device (not reference to the object):

  .. code-block:: python

    from ophyd.sim import det1, det2, det3
    # Assume that the detectors 'det1', 'det2', 'det3' are in the list
    #   of allowed devices for the user submitting the plan.

    def plan_demo4(detector_names, npts):
        # The parameter 'detector_names' is expected to receive a list of detector names.
        <code implementing the plan>

    def plan_demo5(detector_names: typing.List[str], npts):
        # The parameter 'detector_names' is expected to receive a list of detector names.
        <code implementing the plan>

  If the value ``"detector_names": ["det1", "det3"]`` is passed to the plan ``plan_demo4``,
  then the detector names are converted to references, which is not desirable. Annotating
  the type of parameter ``detector_names`` (see ``plan_demo5``) will prevent names from
  being converted to references. This also enables type validation for parameter
  ``detector_names`` and the plan is going to be rejected by Queue Server if the value
  is not a list of strings.

* The name of the device or plan that is passed to the plan is not a name of an existing object
  or is not in the list of allowed plans and devices. For example, assume that
  ``"detectors": ["det1", "det4"]`` is passed to ``plan_demo3``. The detector ``det4``
  is not present in RE Worker namespace, so it will not be converted to reference. As a result,
  the ``detectors`` parameter will receive a list ``[det1, "det4"]`` and the plan will fail
  during execution. In many cases this behavior is not desired and it is preferred that
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
    def plan_demo6(detectors, npts):
        # The parameter 'detector_names' is expected to receive a list of detector names.
        <code implementing the plan>

  The type annotation in the decorator overrides the type annotation in the function header.
  Type validation is now enabled, so only the lists of device names from predefined set will now be
  accepted by the server. If the submitted plan contains ``"detectors": ["det1", "det4"]``,
  the plan is rejected.

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

