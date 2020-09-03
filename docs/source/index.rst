.. Packaging Scientific Python documentation master file, created by
   sphinx-quickstart on Thu Jun 28 12:35:56 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

===================================
 bluesky-queueserver Documentation
===================================

.. toctree::
   :titlesonly:

   installation
   usage
   release-history
   min_versions


.. warning::

   This is currently under rapid development, the API may change at
   any time.



Remote and multi-tenant data acquisition
========================================

In the traditional modes of experimental data collection the user
physically comes to the beamline.  The user has sole control of the
beamline during their beamtime (enforced by their physical presence at
the beamline!) which greatly reduces or makes moot many locking,
coordination, and security issues.  The physical presences at a
beamline work station also causes a blurring of several distinct tasks
that are being done simultaneously.  The urgent move to more remote
experimental data collection means we need to pull apart these tasks
and systematically address the permission issue.

One possible solution is to use some sort of remote desktop
application (VNC, NoMachine, ...) or SSH access to the beamline machines.
While this may be sufficient for simple beamline operations, there are
performance concerns if widely used, it would require additional
infrastructure to make sure that multiple groups do not try to use the
beamline at the same time, and it gives remote users completely
unrestricted access to the beamline machines.



Components of Remote Acquisition
--------------------------------

There are three distinct tasks involved in data acquisition

1. monitoring the beamline status (at least 0.5Hz update rate)
2. invoking the acquisition to collect the data
3. reviewing / processing / interacting with the just collected data

that each have different requirements for interactivity / multi-tenant
/ authentication / latency and need to be addressed independently as
part of remote data acquisition.

Beamline status
~~~~~~~~~~~~~~~

During data collection users need to have access to at some high-level indications
of what the beamline is doing:

- is the RE running or idle?
- is the shutter open?
- is the ring up?
- where are some key motors?

which is best handled by something that will re-publish from EPICS to something
that can be exposed to the outside world.  There are a variety of ways this can be
done, including full web-based OPIs, industry standard logging tools, or simple
webpages.

If this also provides a high-level of engineering control to change individual PVs
is out of scope for this discussion.

Invoke Acquisition
~~~~~~~~~~~~~~~~~~

This is running the beamline in an orchestrated way to acquire the
data that the user wants.  In the bluesky context this mean passing a
plan to the `~bluesky.run_engine.RunEngine`.  Doing this robustly for
(multiple) remote user is the primary goal of this package, see below
for more details.


Review Data
~~~~~~~~~~~

The users need some way to _promptly_ review the data that they have
collected, possibly running some preliminary data reduction.


Proposed implementation
-----------------------


Beamline status
~~~~~~~~~~~~~~~

This is best handled by something that will re-publish from EPICS to
something that can be exposed to the outside world.  There are a
variety of ways this can be done, including full web-based OPIs,
industry standard logging tools, or simple webpages.

If this also provides a high-level of engineering control to change
individual PVs is out of scope for this discussion.


Invoke Acquisition
~~~~~~~~~~~~~~~~~~

There are several scales of granularity that you can think about data acquisition:

0. physical access (you need to go in with a wrench and move things by hand)
1. direct access to the control system (i.e. reading / writing single PVs)
2. device level (i.e. ophyd objects, tango devices)
3. procedure level (i.e. bluesky plans, recursively procedures of procedures...)

At each scale we depend on the lower level.

In line with standard security practice we want to provide users with
the minimum level of access that will let them achieve what they need
to.  To that end we are going to focus on providing remote access at
the procedure leve (4), i.e. the level of bluesky plans.

Because bluesky has the concept of specifying what you want to do
(calling the plan and passing it ophyd objects to get a generator)
from executing the plan (passing the generator to a RunEnigine) it is
natural to use the procedural level as the abstraction level to expose
for remote operations.  We will split the implementation into (at
least) 2 processes, one which handles the business logic / user
interaction / sessions and one which handles actually running the
plan.  By separating these two concerns we can develop them in
parallel and allow for a diversity of front ends that are used to
drive the acquisition.


It is in principle possible to build a full set of proxy devices and
procedures (as is done in Taurus), however this brings with it
significantly higher complexity of the communication between the
client and the server and gives (remote) user more privileges than
they strictly need.


Review Data
~~~~~~~~~~~

The proposed method for accessing "prompt" data is an externally accessible
jupyterhub instance.  We can also use the same processing nodes to manage data
export to a format of the user's choice.


.. _design:


bluesky-queueserver design
==========================



In the standard mode of operation the ``RunEngine``, the ophyd
objects, and the connections to EPICS all live in the same process
that user runs on the beamline machine.  This assumes exclusive control of the
beamline, which when run _from_ the beamline is easy to guarantee.

To enable remote operation we propose a server which owns the
RunEngine instance and is the single (authenticated) entry point for
all users to the beamline.  This gives us:

- the ability to enforce exclusive control of the beamline
- expose the ability to run scans without giving the users full access
  to the beamline
- the ability to provide per-user profiles of what devices and plans
  the user has access to


High-level design
-----------------

::

   RE worker <----> api server <--|edge-of-campus|--> client (web page) <-> user
                       |
                       |-- databases (sample + admin)
                       |-- queue




RE worker
~~~~~~~~~

This is the process that holds the RE + ophyd objects + EPICs connections.
We want this to be independent from the web server so that:

- in case we drive the RE to an invalid state, we want to be able to
  restart the RE without also restarting the web server.
- makes it possible to re-launch the worker with the device / plan
  profile of the current operator
- makes it possible to launch the RE worker
  process as the unix user that matches the current operator


Currently the communication protocol between the worker and the server is
the plan running in the RE polls the server to get the next plan to be run
and the communicate via json.

This process has (almost) no persistent state (the scan_id still needs
to be externalized), everything it knows is either injected at start up time
or comes in with the "next plan" information from the web server.

This process is responsible for translating the serialized version (aka "names")
of the plans / devices into live devices / plans and then running it.

Information that this worker needs to get
.........................................

At start up:

1. what devices / plans are allowed
2. where to publish RE state to
3. where to publish data to
4. where to update the scan_id

While running (which should be hand-shake interactions):

1. "the next plan"
2. interruptions / resumptions
3. changes to the loaded plans / devices

Information that this worker needs to expose
............................................

Everything that needs to be exposed from this process should be
published via one of the channels that are passed in at startup.  We
do not want to burden this worker with significant
non-run-the-event-loop work both to avoid the risk of slowing down
data acquisition and because we expect these workers to be transient
and spawned by the web server.  We do not want to have to coordinate
communication with any other entities (they should instead subscribe to
the location where the information is published instead)


Web server
~~~~~~~~~~

This is where the business logic of the remote operation lives.  This process
will be responsible for sorting out who the user is, what they are allowed to do,
managing the queue of things that need to be done, etc.

This process will take commands from the user like

- add this plan to the queue
- re-order / remove things from the queue
- update persistent meta-data (to go onto all plans)
- interruptions to currently running scan
- start / end / steal exclusive control
- update the devices / plans available to the RE

This process will need to manage the RE worker process so that it can be
(re) launched with the right profile / configuration loaded and it can be
interrupted.

This process will need to expose back to the user

- the current RE state (or where to get it)
- recently taken data (or where to get it)
- the current state of the queue
- if the current session is "live" and has control of the beamline
- the persistent meta-data

This should be developed in line with standard REST API conventions to star with
and possibly a graphql API in the future.


Client
~~~~~~

The clients can be anything that can post/get json to a https endpoint
and understands the protocols the sever exposes.  Possible clients are:

* httpi / curl
* a Python cli tool built around a "restor"
* a PyQt application tool built around a "restor"
* a web application
* an autonomous agent
* Java base GUIs
* ...

We expect there to be wide range of client that interact with server
of varying levels of complexity.  If we get the web server correct we
will be able to develop the clients independently.
