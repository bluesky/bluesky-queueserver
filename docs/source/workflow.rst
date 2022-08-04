======================
Using the Queue Server
======================

Starting and Stopping Run Engine Manager
----------------------------------------

Opening and Closing the Worker Environment
------------------------------------------

Managing the Plan Queue
-----------------------

Controlling Execution of the Queue and the Plans
------------------------------------------------

Interacting with the Worker Environment
---------------------------------------


.. _locking_re_manager:

Locking RE Manager
------------------

Users and client applications can temporarily lock RE Manager. When the manager is locked, users
can access certain groups of API only by pass a *lock key* with API requests. The *lock key* is
an arbitrary string selected by the user who locks RE Manager and stays valid until the manager
is unlocked. The key could be shared with other users who need to control the locked manager.
The lock status is stored in Redis. Restarting the manager does not reset the lock. If the manager
is locked, it needs to be unlocked using valid lock key. Optionally, the emergency key may be set
using the environment variable ``QSERVER_EMERGENCY_LOCK_KEY_FOR_SERVER``. The emergency key allows
to unlock the manager in case the lock key is lost. It can not be used to control the locked RE Manager.

The :ref:`method_lock` API allows to lock the API that control RE Worker environment and/or the queue.
The lock does not affect *read-only* API, therefore monitoring client applications will continue
working when the manager is locked. The full list of API affected by locking the environment and
the queue can be found in the documentation for :ref:`method_lock` API.

The lock is not designed to be used for access control. The typical use case scenarios:

- A beamline scientist or on-site user locks the environment before entering the hutch to change samples.
  This prevents remote users, autonomous agents etc. to open/close the environment, start the queue and
  execute plans and tasks. If necessary, the scientist who locked the environment may still perform
  those operations using the secret lock key without unlocking the manager. Since the queue is not locked,
  the remote users and autonomous agents are still free to edit the queue or add plans to the queue.

- A beamline scientist is performing maintenance or calibration and locks both the environment and
  the queue to have exclusive control of the manager.

API for controlling and monitoring lock status of the manager:

- :ref:`method_lock` - lock the environment and/or the queue using a lock key. The API also accepts
  the name of the user who locks the manager (required) and a text note to other users (optional).
  This information is returned as part of the lock info and included in all relevant error messages.

- :ref:`method_unlock` - unlock the manager using the valid lock key (it must be the same key as
  for locking the manager) or the emergency lock key (if set). If the key is lost and the emergency
  key is not set or unknown, the lock can be cleared using :ref:`qserver_clear_lock_cli` CLI tool
  and restarting RE Manager application or service.

- :ref:`method_lock_info` - load the manager lock status. The lock status is assigned a UID, which
  is updated each time the status is changed. The UID is included in the manager status (:ref:`method_status` API),
  which simplifies monitoring of the lock status. The manager status also contains *'lock'* parameter,
  which indicates if the environment and/or the queue are currently locked.

The operations of locking and unlocking RE Manager using CLI tool could be found in the tutorial
:ref:`tutorial_locking_re_manager`.

.. note::

  The :ref:`method_lock` API controls access to other API, not internal operation of the server.
  For example, if the server is executing the queue, the queue will continue running after
  the manager is locked until it runs out of plans or stopped.

