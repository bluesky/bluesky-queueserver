from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

from .manager.comms import ZMQCommSendAsync, ZMQCommSendThreads  # noqa: E402, F401
