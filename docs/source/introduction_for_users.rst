============
Introduction
============

What is Bluesky Queue Server?
-----------------------------

Bluesky Queue Server is a set of tools that provide alternative method for executing
`Bluesky <https://blueskyproject.io/bluesky>`_ plans. The Queue Server includes the core
`bluesky-queueserver <https://github.com/bluesky/bluesky-queueserver>`_ package and
`bluesky-queueserver-api <https://github.com/bluesky/bluesky-queueserver-api>`_ and
`bluesky-httpserver <https://github.com/bluesky/bluesky-httpserver>`_ packages that implement
additional functionality. Traditionally Bluesky was used interactively
from IPython environment as demonstrated in
`Bluesky tutorials <https://blueskyproject.io/bluesky/tutorial.html>`_.
A session would start by loading startup scripts that define beamline-specific devices and plans
(see a typical example of Bluesky startup code used by
`NSLS2 SRX beamline <https://github.com/NSLS-II-SRX/profile_collection/tree/master/startup>`_)
and then interactively running plans one-by-one from IPython prompt or executing a script that
runs a sequence of plans. Interactive IPython workflow is currently the most common way to
use Bluesky.

Alternatively, the Queue Server allows to run Bluesky in a dedicated Python process
(Run Engine worker environment). The worker environment is created and managed by Run Engine (RE)
Manager (see :ref:`start_re_manager_cli`), which could be run as an application or a service.
As in IPython workflow, the startup code is loaded into the environment and
the beamline devices and plans are available in the environment namespace. Bluesky plans
are executed by populating and starting the plan queue maintained by RE Manager. The queue is
editable and can be modified by users at any time: queue items may be added to the queue,
replaced (edited), moved to different positions and removed from the queue.

Queue Server is designed to accept startup scripts developed for interative IPython
workflow after minor modifications. It is acknowledged that the maintenance of
startup scripts takes a lot of effort. The startup scripts adapted for Queue Server
can always be loaded in IPython environment used in both Queue Server and interactive
IPython workflows without modifications.

RE Manager is fully controlled over 0MQ using a :ref:`comprehensive set of API <run_engine_manager_api>`,
which allows to open and close the worker environment, control the plan queue, monitor the state of RE Manager,
execute scripts and functions, etc. The API-controlled execution environment does not provide flexibility
of IPython-based workflow, but it is more convenient for the development of GUI applications or autonomous
agents for local or remote control of experiments. RE Manager is designed to be self-sufficient and
may complete execution of populated plan queue without user intervention, allowing to decouple control GUI
or agent from the plan execution engine: in case of failure, the higher level control software could be restarted
without interfering with the running experiment. User applications or control agents may access RE Manager
remotely using 0MQ API on the local network (as long as RE Manager may be accessed from the machine running
the control software). `Bluesky HTTP Server <https://blueskyproject.io/bluesky-httpserver/>`_ is designed to
control RE Manager from outside the local network and provides a matching set of REST API and basic authentication,
authorization and access control. The HTTP Server communicates with RE Manager over 0MQ and needs access to
the local network.

Using low-level 0MQ API (RE Manager) or REST API (HTTP Server) in Python client applications
or scripts may not be convenient: the API are not user-friendly and proper use of API
may require understanding of internals of RE Manager. Also 0MQ and REST API are implemented using
different incompatible libraries, therefore two versions of communication code need to be developed and
maintained to support both protocols (e.g. an application that can work locally via 0MQ and remotely via HTTP).
The higher level Python API package
(`Bluesky Queue Server API <https://blueskyproject.io/bluesky-queueserver-api>`_)
was developed to support more convenient and universal interface for communication with
RE Manager. The package contains API for synchronous and asynchronous (*asyncio*) communication over 0MQ and HTTP.
The API hides many low-level communication details and allows to write the code that works identically with both
protocols (except initialization parameters, which are different for 0MQ and HTTP).

The *bluesky-queueserver* package provides a simple CLI application (:ref:`qserver_cli`)
for controlling RE Manager over 0MQ. The application may be useful for testing
RE Manager, troubleshooting issues and simple demos. The :ref:`tutorials <tutorials_queue_server>`
explore the features of the Queue Server using *qserver* CLI tool.

Features of Bluesky Queue Server
--------------------------------

Integration of Bluesky Queue Server in Data Acquisition system
--------------------------------------------------------------

.. image:: images/qserver-diagram.png
    :alt: Diagram of Bluesky Queue Server
