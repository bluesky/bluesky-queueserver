.. _organizing_bluesky_startup_code:

===============================
Organizing Bluesky Startup Code
===============================

.. currentmodule:: bluesky_queueserver

.. _detecting_if_code_executed_by_re_worker:

Detecting if the Code is Executed by RE Worker
----------------------------------------------

Calling the ``is_re_worker_active()`` API anywhere in the startup code or an imported module
allows to detect if the code is executed by RE Worker. The function returns ``True`` if
the code is running in RE Worker environment and ``False`` otherwise.

The API may be used to conditionally avoid execution of some code (e.g. the code that interacts
with the user) when experiments are controlled remotely:

.. code-block:: python

    from bluesky_queueserver import is_re_worker_active

    ...

    if is_re_worker_active():
        <code without interactive features, e.g. reading data from a file>
    else:
        <code with interactive features, e.g. manual data input>

    ...

.. note::

    Queue Server provides additional ``set_re_worker_active()`` and ``clear_re_worker_active()`` API,
    which modify the value returned by subsequent calls to ``is_re_worker_active()`` in the current process.
    These API are intended for implementation of tests and **should never be used in startup scripts**.

.. autosummary::
    :nosignatures:
    :toctree: generated

    is_re_worker_active
    set_re_worker_active
    clear_re_worker_active


.. _detecting_if_code_executed_in_ipython:

Detecting if the Code is Executed in IPython Kernel
---------------------------------------------------

The function ``is_ipython_mode()`` returns ``True`` if the code is executed in IPython environment
and ``False`` if the environment is pure Python. The function corectly detects IPython
environment even if the code with patched ``IPython.get_ipython()`` is loaded.
The function is used similarly to ``is_re_worker_active()``. The functions may be used
in startup code and uploaded scripts to identify whether the code is run in the worker environment
and whether the IPython features can be used.


.. autosummary::
    :nosignatures:
    :toctree: generated

    is_ipython_mode
    set_re_worker_active
    clear_re_worker_active
