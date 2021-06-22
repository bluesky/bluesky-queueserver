# ===============================================================================================
#   This module is temporarily used by the Queue Server code.
#   Do not import anything from this module!
#
#   The code in this file is vendored from 'bluesky.protocols' module. This module will be removed
#   once the respective Bluesky PR is merged and the new version of Bluesky that supporting
#   'bluesky.protocols' is released and widely deployed.
# ===============================================================================================

try:
    from typing import Protocol, runtime_checkable
except ImportError:
    from typing_extensions import Protocol, runtime_checkable

from typing import Dict, Any, Optional, Callable, Generator, NoReturn


Configuration = Dict[str, Dict[str, Any]]


@runtime_checkable
class Status(Protocol):
    done: bool
    success: bool

    def add_callback(self, callback: Callable[["Status"], NoReturn]) -> NoReturn:
        """Add a callback function to be called upon completion.
        The function must take the status as an argument.
        If the Status object is done when the function is added, it should be
        called immediately.
        """
        ...


@runtime_checkable
class Readable(Protocol):
    name: str

    @property
    def parent(self) -> Optional[Any]:
        """``None``, or a reference to a parent device."""
        ...

    def trigger(self) -> Status:
        """Return a ``Status`` that is marked done when the device is done triggering.
        If the device does not need to be triggered, simply return a ``Status``
        that is marked done immediately.
        """
        ...

    def read(self) -> Configuration:
        """Return an OrderedDict mapping field name(s) to values and timestamps.
        The field names must be strings. The values can be any JSON-encodable
        type or a numpy array, which the RunEngine will convert to (nested)
        lists. The timestamps should be UNIX time (seconds since 1970).
        Example return value:
        .. code-block:: python
            OrderedDict(('channel1',
                         {'value': 5, 'timestamp': 1472493713.271991}),
                         ('channel2',
                         {'value': 16, 'timestamp': 1472493713.539238}))
        """
        ...

    def describe(self) -> Configuration:
        """Return an OrderedDict with exactly the same keys as the ``read``
        method, here mapped to metadata about each field.
        Example return value:
        .. code-block:: python
            OrderedDict(('channel1',
                         {'source': 'XF23-ID:SOME_PV_NAME',
                          'dtype': 'number',
                          'shape': []}),
                        ('channel2',
                         {'source': 'XF23-ID:SOME_PV_NAME',
                          'dtype': 'number',
                          'shape': []}))
        We refer to each entry as a "data key." These fields are required:
        * source (a descriptive string --- e.g., an EPICS Process Variable)
        * dtype: one of the JSON data types: {'number', 'string', 'array'}
        * shape: list of integers (dimension sizes) --- e.g., ``[5, 5]`` for a
          5x5 array. Use empty list ``[]`` to indicate a scalar.
        Optional additional fields (precision, units, etc.) are allowed.
        The optional field ``external`` should be used to provide information
        about references to externally-stored data, such as large image arrays.
        """
        ...

    def read_configuration(self) -> Configuration:
        """Same API as ``read`` but for slow-changing fields related to configuration.
        (e.g., exposure time)
        These will typically be read only once per run.
        Of course, for simple cases, you can effectively omit this complexity
        by returning an empty dictionary.
        """
        ...

    def describe_configuration(self) -> Configuration:
        """Same API as ``describe``, but corresponding to the keys in ``read_configuration``."""
        ...


@runtime_checkable
class Movable(Readable, Protocol):
    def set(self, value) -> Status:
        """Return a ``Status`` that is marked done when the device is done moving."""
        ...


@runtime_checkable
class Flyable(Protocol):
    name: str

    @property
    def parent(self) -> Optional[Any]:
        """``None``, or a reference to a parent device."""
        ...

    def kickoff(self) -> Status:
        """Begin acculumating data.
        Return a ``Status`` and mark it done when acqusition has begun.
        """
        ...

    def complete(self) -> Status:
        """Return a ``Status`` and mark it done when acquisition has completed."""
        ...

    def collect(self) -> Generator[Dict[str, Any], None, None]:
        """Yield dictionaries that are partial Event documents.
        They should contain the keys 'time', 'data', and 'timestamps'.
        A 'uid' is added by the RunEngine.
        """
        ...

    def describe_collect(self) -> Dict[str, Configuration]:
        """This is like ``describe()`` on readable devices, but with an extra layer of nesting.
        Since a flyer can potentially return more than one event stream, this is a dict
        of stream names (strings) mapped to a ``describe()``-type output for each.
        """
        ...

    # Remaining same as Readable
    def read_configuration(self) -> Configuration:
        """Same as for a Readable device."""
        ...

    def describe_configuration(self) -> Configuration:
        """Same as for a Readable device."""
        ...
