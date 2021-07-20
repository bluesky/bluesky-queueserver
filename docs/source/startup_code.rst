===============================
Organizing Bluesky Startup Code
===============================

.. currentmodule:: bluesky_queueserver

Documentation is coming soon ...

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
