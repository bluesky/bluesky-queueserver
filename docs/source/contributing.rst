============
Contributing
============

Development Installation
------------------------

Install Redis and create Conda environment as described in :ref:`installation_steps`.

Install the Queue Server in editable mode::

  $ pip install -e .

Install development dependencies::

  $ pip install -r requirements-dev.txt


Setting up `pre-commit`
-----------------------

`pre-commit`` package is installed as part of the development requirements. Install pre-commit
script by running ::

  $ pre-commit install

Once installed, `pre-commit` will perform all the checks before each commit. As the new versions
of validation packages are released, the pre-commit script can be updated by running ::

  $ pre-commit autoupdate


Running Unit Tests Locally
--------------------------

The Queue Server must be tested separately with disabled IPython mode::

  $ pytest -vvv

or ::

  $ USE_IPYKERNEL=false pytest -vvv

and enabled IPython mode::

  $ USE_IPYKERNEL=true pytest -vvv


Running Unit Tests on GitHub
----------------------------

Execution of the full test suite on CI takes too long and causes major inconvenience,
therefore it is split into multiple groups (currently 3 groups) using `pytest-split`
package. Since the goal is to reduce the execution time of the longest group, the
splitting algorithm is calibrated based on execution time of the tests with enabled
IPython kernel mode (more tests, each test takes a little longer to execute).
Calibration is performed by running the script ``store_test_durations.sh`` locally,
which saves execution time for each test in the ```.test_durations`` file. The file then
has to be committed and pushed to the repository.

`pytest-split` will automatically guess execution time for new tests that are not
listed in ``.test_durations`` file, so calibration may be needed rarely or after major
changes to the test suite and is expected to be performed by the package maintainers.
