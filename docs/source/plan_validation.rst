===============
Plan Validation
===============

.. currentmodule:: bluesky_queueserver

.. _plan_validation:

Plan Validation
---------------

All the submitted plans are subjected to validation before they are added to the queue. If plan
validation  fails then the plan is rejected. If a batch of plans is submitted to the queue
(e.g. using :ref:`queue_item_add_batch <method_queue_item_add_batch>` 0MQ API), each plan in
the batch has to be validated before the plans are added to the queue. If validation of
any plan in the batch fails, then the whole batch is rejected.

Validation is using data including plan name, plan parameters and the name of the user group
to verify if plan parameters are valid and the user is allowed to submit the plan.
Plan validation includes the following steps:

* Check that the plan name is in the list of allowed plans for the user submitting the plan.
  (Each user is assigned to a group. The name of the user group is passed as part of
  the API parameters).

* Check that the set of plan parameters, passed with the API call as a list of args and a dictionary
  of kwargs, can be successfully passed to the plan. Validation procedure attempts to bind
  the passed arguments to plan parameters and verifies that parameter types match hte parameter
  annotation.

* Check that the user is allowed to use plans and devices that are passed as plan parameters.
  If the plan accepts other plans or devices as parameter values, check that they are in
  the lists of allowed plans and devices for the user group.

  .. note::
    Only names of plans and devices can be passed as parameters in API requests. The names are replaced
    by the respective objects from RE Worker namespace before the parameters are passed to the plan.

Validation of plans is performed without access to the RE Worker namespace and
based exclusively on the data on plans and devices stored in the file *'existing_plans_and_devices.yaml'*.

Each plan is validated at least twice: at the time it is submitted to the Queue Server and
before execution. Validation is also run each time the plan parameters are modified
(see :ref:`queue_item_update <method_queue_item_update>` 0MQ API). Third party Python applications
may perform validation before sending the plan to the queue by calling `validate_plan()`
(:ref:`see below <plan_validation_api>`).

.. note::

  If a plan is successfully submitted to the queue, it is expected to pass validation before
  execution. If the contents of the list of existing devices (*'existing_plans_and_devices.yaml'*)
  or user group permissions (*'user_group_permissions.yaml'*) are modified while the plan is
  in the queue, validation may still fail.

.. _plan_validation_api:

API for Plan Validation
-----------------------

.. autosummary::
   :nosignatures:
   :toctree: generated

    validate_plan

