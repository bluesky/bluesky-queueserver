=========================
Validation of Queue Items
=========================

.. currentmodule:: bluesky_queueserver

All items (plans and instructions) submitted to the queue subjected to validation
before they are inserted into the queue. If validation of an item fails,
the item is rejected and an error message is returned as part of the response to
the API request. If a batch of items is submitted to the queue (e.g. using
:ref:`queue_item_add_batch <method_queue_item_add_batch>` 0MQ API), each item in
the batch is validated before any items are added to the queue. If validation of
any item in the batch fails then the whole batch is rejected.


Validation of Instructions
--------------------------

A submitted item can be a plan or an instruction. Validation of instructions is relatively
simple because the set of instructions and their parameters is fixed. Validation involves
the following steps:

- Verify that the instruction name is a name of an existing instruction.

- Verify that the instruction name is in the list of allowed instructions for the user
  submitting the plan (to be implemented).

- Verify that the submitted request contains all the required instruction parameters.

- Verify that the submitted request contains no parameters that are not supported.

- Verify that the submitted parameter values are valid.


.. _plan_validation:

Validation of Plans
-------------------

Validation of plan parameters is more sophysticated and based on plan representations
stored in the file ``existing_plans_and_devices.yaml``, which is created by analysing
the startup script(s) or the module with :ref:`qserver_list_plans_devices_cli` CLI tool.
Alternatively, RE Manager may be configured to create/update the list of
existing plans and devices stored on disk each time RE Worker environment is opened or
plans and devices in RE namespace are changed. See :ref:`update_existing_plans_devices`
for available options. The lists of existing plans and device used by RE Manager
(stored internally in RAM) are always kept up to date during runtime to reflect changes
in RE namespace. The lists are reloaded from disk file when RE Manager is started and
used until RE Worker environment is opened the first time and startup scripts are
successfully loaded, therefore it is important to keep ``existing_plans_and_devices.yaml``
up-to-date.

Each user is assigned to a user group. Each user group is assigned permissions
to use a subset of names of existing plans and devices in API requests. The permissions
are defined in the file ``user_group_permissions.yaml``. The lists of allowed plans and
devices for a given group are generated from the lists of existing plans and devices
based on the respective set of permissions. Plan validation includes the following steps:

* Verify that the plan name is in the list of allowed plans for the user submitting the plan.

* Verify that the set of submitted plan parameters (a list of ``args`` and a dictionary
  of ``kwargs``) can be successfully passed to the plan: attempt to bind
  ``args`` and ``kwargs`` to plan parameters using plan signature recreated based from
  the plan representation found in the list of allowed plans; check parameter types based
  on the type annotations (if specified) and check if numerical values are within
  the allowed ranges (if specified). This step does not guarantee that the plan will
  run successfully, but may catch many parameter errors.

* Verify that the user is allowed to use plans and devices that are passed as plan parameters
  (i.e. check if the plans and devices are in the lists of allowed plans and devices).

  .. note::
    Only names of plans and devices can be passed as parameters in API requests.
    The names are replaced by the respective objects from RE Worker namespace before
    the parameters are passed to the plan.

Each plan is validated at least twice: at the time the plan is submitted to the Queue Server and
directly before execution. Validation is also run each time plan parameters are modified
(see :ref:`queue_item_update <method_queue_item_update>` 0MQ API). Third party Python applications
may perform validation before sending the plan to the queue by calling ``validate_plan()``
(:ref:`see below <plan_validation_api>`).

.. note::

  Once a plan is successfully submitted to the queue, it is expected to successfully pass
  the second validation immediately before it is set for execution. Nevertheless, if
  the lists of existing plans and devices or user group permissions are modified between
  the time when the plan was added to the queue and the time the plan is set for execution,
  the second validation may fail.

.. _plan_validation_api:

API for Plan Validation
-----------------------

.. autosummary::
   :nosignatures:
   :toctree: generated

    validate_plan
