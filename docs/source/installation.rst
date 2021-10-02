============
Installation
============

System Requirements
-------------------

Supported Python versions: 3.7, 3.8.

Installation Steps
------------------

* **Install Redis**

  Skip this step if **Redis** is already installed.

  Linux::

    $ sudo apt install redis

  Mac OS:

    See https://gist.github.com/tomysmile/1b8a321e7c58499ef9f9441b2faa0aa8.

* **Create Conda environment**

  It is recommended to install the Queue Server in Conda environment. A new environment can be created
  or already existing environment can be activated. The following example illustrates how to create
  a new Conda environment with the name *queue_server* and Python 3.7 installed::

    $ conda create -n queue_server python=3.7
    $ activate queue_server

* **Install Queue Server**

  Install from PyPI::

    $ pip install bluesky-queueserver

  Install from conda-forge::

    $ conda install bluesky-queueserver -c conda-forge

  Install from from source (GitHub repository)::

    $ cd <directory-with-git-repositories>
    $ git clone https://github.com/bluesky/bluesky-queueserver.git
    $ cd bluesky-queueserver
    $ pip install -e .

  After the installation is completed, users should be able to run ``start-re-manager``, ``qserver``,
  ``qserver-list-plans-devices`` and ``qserver-zmq-keys`` tools from command line.
