Annotation of Bluesky Plans
===========================

Plan Validation
---------------

All plans submitted to the queue are subjected to validation. In case validation of a plan fails,
the plan is rejected. Validation is repeated before the plan is sent for execution. When a batch
of plans is submitted to the queue, each plan in the batch is validated and the whole batch is
rejected if validation of at least one of the plans fails. The validation of a plan includes
the following checks:

* User is allowed to submit the plan (plan name is in the list of allowed plans for the user group).

* Submitted plan parameters are accepted by the plan: the request contains values for all required
  parameters and all value types match the types of plan parameters. Plans and devices passed as parameters
  must be in the lists of allowed plans and devices respectively.

  .. note::
    Only names of plans and devices can be passed as parameters in API requests. The names are replaced
    by the respective objects from RE Worker namespace before the parameters are passed to the plan.

Validation of plans is performed without access to plans and devices from RE Worker namespace and
based exclusively on the data from the file ``existing_plans_and_devices.yaml``.