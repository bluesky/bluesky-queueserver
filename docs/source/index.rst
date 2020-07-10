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
beamline during their (enforced by their physical presence at the
beamline!) which greatly reduces or makes moot many locking,
coordination, and security issues.  The physical presences at a
beamline work station also causes a blurring of several distinct tasks
that are being done simultaneously.  The urgent move to more remote
experimental data collection means we need to pull apart these tasks and
systematically address the permission issue.

One possible solution is to use some sort of remote desktop
application (VNC, NoMachine, ...) or SSH access to the beamline machines.
While this may be sufficient for simple beamline operations, there are
performance concerns if widely used, would require addition
infrastructure to make sure that multiple groups do not try to use the
beamline at the same time, and it gives remote users completely
unrestricted access to the beamline machines.



Components of Remote Acquisition
--------------------------------

There are three distinct tasks in involved in data acquisition

1. monitoring the beamline status
2. invoking the acquisition to collect the data
3. reviewing the just collected data

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
the level of plans.

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
export to a format of the users choice.


.. _design


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

   RE worker <--queue--> web server <--|edge-of-campus|--> web page <-> user





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

While running:

1. "the next plan"
2. interruptions


Web server
~~~~~~~~~~

This is where the business logic of the remote operation lives.  This process
will be responsible for sorting out who the user is, what


Open work
---------

RE worker
~~~~~~~~~

1. use logging hooks to publish state
2. harden the json -> plan + object code
3. develop way to describe the allowed plans and devices
4. happi integration
5. ability to add additional plans / devices
6. ability to send a Python module of plans / devices to add
7. re-think communication methods

Server
~~~~~~

0. add logic to server to spawn RE worker with state based on current user
   a. what plans
   b. what devices
   c. where to publish documents to
   d. where to publish RE state to
   e. nanny process to restart etc
1. add ability to publish the list of plans and devices available to a given user
2. make queue mutable other than addition
3. authentication + session logic
4. business logic to de-conflict usage
5. put the queue in a persistent data structure
6. per-user persistent meta-data

Web page / front end
~~~~~~~~~~~~~~~~~~~~

0. exist 🤷
