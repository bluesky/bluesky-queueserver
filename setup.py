from os import path
from setuptools import setup, find_packages
import sys
import versioneer


# NOTE: This file must remain Python 2 compatible for the foreseeable future,
# to ensure that we error out properly for people with outdated setuptools
# and/or pip.
min_version = (3, 7)
if sys.version_info < min_version:
    error = """
bluesky-queueserver does not support Python {0}.{1}.
Python {2}.{3} and above is required. Check your Python version like so:

python3 --version

This may be due to an out-of-date pip. Make sure you have pip >= 9.0.1.
Upgrade pip like so:

pip install --upgrade pip
""".format(
        *(sys.version_info[:2] + min_version)
    )
    sys.exit(error)

here = path.abspath(path.dirname(__file__))

with open(path.join(here, "README.rst"), encoding="utf-8") as readme_file:
    readme = readme_file.read()

with open(path.join(here, "requirements.txt")) as requirements_file:
    # Parse requirements.txt, ignoring any commented-out lines.
    requirements = [line for line in requirements_file.read().splitlines() if not line.startswith("#")]

setup(
    name="bluesky-queueserver",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Server for queueing plans",
    long_description=readme,
    author="Brookhaven National Laboratory",
    author_email="",
    url="https://github.com/bluesky/bluesky-queueserver",
    python_requires=">={}".format(".".join(str(n) for n in min_version)),
    packages=find_packages(exclude=["docs", "tests"]),
    entry_points={
        "console_scripts": [
            "qserver = bluesky_queueserver.manager.qserver_cli:qserver",
            "start-re-manager = bluesky_queueserver.manager.start_manager:start_manager",
            "qserver-list-plans-devices = bluesky_queueserver.manager."
            "profile_ops:gen_list_of_plans_and_devices_cli",
            "qserver-zmq-keys = bluesky_queueserver.manager.qserver_cli:qserver_zmq_keys",
            "qserver-console-monitor = bluesky_queueserver.manager.output_streaming:qserver_console_monitor_cli",
        ],
    },
    include_package_data=True,
    package_data={
        "bluesky_queueserver": [
            "profile_collection_sim/*"
            # When adding files here, remember to update MANIFEST.in as well,
            # or else they will not be included in the distribution on PyPI!
            # 'path/to/data_file',
        ]
    },
    install_requires=requirements,
    license="BSD (3-clause)",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
    ],
)
