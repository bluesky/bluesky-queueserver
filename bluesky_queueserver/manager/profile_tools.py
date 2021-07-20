import functools
import inspect
import re
import os


class UserNamespace:
    """
    Keeps reference to the global user namespace. The object is used to emulate
    ``IPython.get_ipython().user_ns``. The attribute ``__use_ipython`` holds a
    flag that tells if ``IPython`` (namespace etc.) should be used.
    """

    def __init__(self):
        # Default: use IPython if executed from IPython.
        self.__user_ns = {}
        self.__use_ipython = True

    @property
    def user_ns(self):
        return self.__user_ns

    @user_ns.setter
    def user_ns(self, ns):
        raise RuntimeError("Attempting to set read-only property ``user_ns``")

    @property
    def use_ipython(self):
        return self.__use_ipython

    @use_ipython.setter
    def use_ipython(self, use_ipython):
        raise RuntimeError("Attempting to set read-only property ``use_ipython``")

    def set_user_namespace(self, *, user_ns, use_ipython=False):
        """
        Set reference to the user namespace. Typically the reference will point to
        a dictionary, but it also may be pointed to IPython ``user_ns``. The second
        parameter (``use_ipython``) indicates if IPython is used.

        Parameters
        ----------
        user_ns: dict
            Reference to the namespace.
        use_ipython: boolean
            Indicates if IPython is used. Typically it should be set ``False``.


        Raises
        ------
        TypeError
            Parameter ``user_ns`` is not a dictionary.
        """
        if not isinstance(user_ns, dict):
            raise TypeError(f"Parameter 'user_ns' must be of type 'dict': type(ns) is '{type(dict)}'")
        self.__user_ns = user_ns
        self.__use_ipython = bool(use_ipython)


global_user_namespace = UserNamespace()


def set_user_ns(func):
    """
    The decorator may be used to let the function access Run Engine namespace.
    Note, that the decorator may behave differently if used in start up files,
    because of patching of the ``get_ipython()`` function.

    If the function is executed from IPython, then the namespace is defined as
    ``IPython.get_ipython().user_ns``. If function is executed in python, then
    the namespace is passed as ``global_user_namespace.user_ns``. The object
    ``global_user_namespace`` is only a container for the storage, so its
    property ``user_ns`` should be set to reference RE namespace before the
    function is executed. Note, that the reference may be changed at any time -
    the namespace can be (and often is) different than the one used during
    import.

    The decorated function must have one kwargs: ``user_ns`` - reference
    to RE namespace. An optional kwarg ``ipython`` is a reference to IPython,
    which is None if the function called not from IPython.
    """

    def get_user_ns():
        from IPython import get_ipython

        ip = get_ipython()

        # This is needed for the case when the functions are executed from IPython,
        #   but we needed them to work exactly as if they were run without IPython.
        if not global_user_namespace.use_ipython:
            ip = None

        if ip:
            # Select IPython namespace (only if the code is run from IPython)
            user_ns = ip.user_ns
        else:
            # Select global user namespace that is manually set
            user_ns = global_user_namespace.user_ns
        return ip, user_ns

    # Parameter 'ipython' is optional
    is_ipython_in_sig = "ipython" in inspect.signature(func).parameters

    if inspect.isgeneratorfunction(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            ip, user_ns = get_user_ns()
            kwargs.update({"user_ns": user_ns})
            if is_ipython_in_sig:
                kwargs.update({"ipython": ip})
            return (yield from func(*args, **kwargs))

    else:

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            ip, user_ns = get_user_ns()
            kwargs.update({"user_ns": user_ns})
            if is_ipython_in_sig:
                kwargs.update({"ipython": ip})
            return func(*args, **kwargs)

    params_to_remove = ("user_ns", "ipython")
    sig_params = inspect.signature(wrapper).parameters
    sig = inspect.Signature([sig_params[_] for _ in sig_params if _ not in params_to_remove])
    wrapper.__signature__ = sig

    return wrapper


def load_devices_from_happi(device_names, *, namespace, **kwargs):
    """
    Load the devices from Happi based on the list of ``device_names``. The elements of the list
    may be strings (device name) or tuples of two strings (device name used for database search and
    the name with which the device is loaded in the script namespace). Two names may be needed
    in case the devices are stored in the database using compound names that contain beamline
    acronyms (e.g. ``abc_det`` and ``def_det`` for beamlines ABC and DEF) but they are expected
    to have the name ``det`` when loaded during ABC and DEF beamline startup respectively (see examples).

    The devices are loaded into a namespace referenced by ``namespace`` parameter.
    The function may be called multiple times in a row for the same namespace to populate
    it with results of multiple searches. The function also returns the list of names of loaded devices.

    Happi should be configured before the function could be used: Happi configuration file should be
    created and environment variable ``HAPPI_CFG`` with the path to the configuration file should be set.
    For example, if JSON Happi database is contained in the file ``path=/home/user/happi/database.json``
    and the path to configuration file is ``path=/home/user/happi.ini``, then the environment variable
    should be set as

    .. code-block::

        HAPPI_CFG=/home/user/happi.ini

    and configuration file ``happi.ini`` should contain

    .. code-block::

        [DEFAULT]
        backend=json
        path=/home/user/happi/database.json

    Examples
    --------

    .. code-block:: python

        # Load devices 'det1' and 'motor1'.
        load_devices_from_happi(["det1", "motor1"], namespace=locals())
        # Works exactly the same as the previous example. If the second tuple element is
        #   evaluated as boolean value of False, then it is ignored and the device is not renamed.
        load_devices_from_happi([("det1", ""), ("motor1", "")], namespace=locals())

        # Load 'abc_det1' as 'det1' and 'abc_motor1' as 'motor1'.
        load_devices_from_happi([("abc_det1", "det1"), ("abc_motor1", "motor1")], namespace=locals())

        # Get the dictionary of devices
        device_dict = {}
        load_devices_from_happi(["det1", "motor1"], namespace=device_dict)
        # 'device_dict': {"dev1": device1, "motor1": motor1}

        # Obtain the list of updated items (devices)
        item_list = load_devices_from_happi([("abc_det1", "det1"), ("abc_motor1", "motor1")], namespace=locals())
        # 'item_list': ["det1", "motor1"]

    Parameters
    ----------
    device_names : list(str) or list(tuple(str))
        List of device names. Elements of the list could be strings or tuples (lists) of string with
        two elements. If an element is a tuple of two names, then the first name is used in database
        search and the second name is the name of the device after it is loaded into the namespace.
        Each element of the list is processed separately and may contain mix of strings and tuples.
        If the second name in the tuple is an empty string, then it is ignored and
        the device is imported with the same name as used in database. It is expected that search
        will result in one found device per device name, otherwise ``RuntimeError`` is raised.
    namespace : dict
        Reference to the namespace where the devices should be loaded. It can be the reference
        to ``global()`` or ``locals()`` of the startup script or reference to a dictionary.
        It is a required keyword argument.
    kwargs : dict
        Additional search parameters.

    Returns
    -------
    list(str)
        the list of names of items in the ``namespace`` that were updated by the function.

    Raises
    ------
    RuntimeError
        Search for one of the device names returns no devices or more than one device.
    """

    kwargs = kwargs or {}

    if not isinstance(namespace, dict):
        raise TypeError(
            f"Parameter 'namespace' must be a dictionary: the value of type {type(namespace)} was passed instead"
        )

    # Verify that 'device_names' has correct type
    if not isinstance(device_names, (tuple, list)):
        raise TypeError(
            "Parameter 'device_names' value must be a tuple or a list: "
            f"type(device_names) = {type(device_names)}"
        )
    for n, name in enumerate(device_names):
        if not isinstance(name, (str, tuple, list)):
            raise TypeError(
                f"Parameter 'device_names': element #{n} must be str, tuple or list: " f"device_names[n] = {name}"
            )
        if isinstance(name, (tuple, list)):
            if len(name) != 2 or not isinstance(name[0], str) or not isinstance(name[1], str):
                raise TypeError(
                    f"Parameter 'device_names': element #{n} is expected to be in the form "
                    f"('name_in_db', 'name_in_namespace'): device_names[n] = {name}"
                )
            elif name[1] and not re.search(r"^[a-z][_a-z0-9]*$", name[1]):
                raise TypeError(
                    f"The device '{name[0]}' can not be renamed: The new device name '{name[1]}' "
                    "may consist of lowercase letters, numbers and '_' and must start from lowercase letter"
                )

    from happi import Client, load_devices

    client = Client.from_config()

    results = []
    for d_name in device_names:
        if isinstance(d_name, str):
            name_db, name_ns = d_name, None
        else:
            name_db, name_ns = d_name

        # Assemble search parameters
        search_params = dict(kwargs)
        search_params.update({"name": name_db})

        res = client.search(**search_params)
        if not res:
            raise RuntimeError(
                f"No devices with name '{name_db}' were found in Happi database. "
                f"Search parameters {search_params}"
            )
        elif len(res) > 1:
            raise RuntimeError(
                f"Multiple devices with name '{name_db}' were found in Happi database. "
                f"Search parameters {search_params}"
            )
        else:
            r = res[0]
            # Modify the object name (if needed)
            if name_ns:
                # In order for the following conversion to work properly, the name of
                #   the device should be specified only once (as `name` attribute),
                #   and aliases `{{name}}` should be used if the name is used as any other
                #   parameter. This is standard and recommended practice for instantiating
                #   Happi items (devices).
                #
                # In search results, the reference to the device is `r._device`.
                # `r.metadata` contains expanded metadata (it is probably not used, but it
                #   is a good idea to change it as well for consistency. We don't touch `_id`.
                # The modified data is not expected to be saved to the database.
                setattr(r._device, "name", name_ns)
                r.metadata["name"] = name_ns
            # Instantiate the object
            results.append(r)

    ns = load_devices(*[_.item for _ in results])
    ns_dict = ns.__dict__
    namespace.update(ns_dict)
    return list(ns_dict)


# Name of the environment variable may change in the future. Use API to set/clear the variable.
#   The variable name should not be included in the documentation.
_env_re_worker_active = "QSERVER_RE_WORKER_ACTIVE"


def set_re_worker_active():
    """
    Set the environment variable used to determine if the current process is RE Worker process.
    Subsequent calls to ``is_re_worker_active`` in the current process will return ``True``.
    THIS FUNCTION SHOULD NEVER BE CALLED IN STARTUP SCRIPTS.
    """
    os.environ[_env_re_worker_active] = "1"


def clear_re_worker_active():
    """
    Clear the environment variable used to determine if the current process is RE Worker process.
    Subsequent calls to ``is_re_worker_active`` in the current process will return ``False``.
    THIS FUNCTION SHOULD NEVER BE CALLED IN STARTUP SCRIPTS.
    """
    if _env_re_worker_active in os.environ:
        del os.environ[_env_re_worker_active]


def is_re_worker_active():
    """
    The function can be used in startup scripts or modules to check if the script is imported or
    executed in RE Worker environment. For example, an experimental plan may contain interactive
    features that should be disabled if the plan is executed remotely:

    .. code-block:: python

        from bluesky_queueserver import is_re_worker_active

        ...

        if is_re_worker_active():
            (code without interactive features, e.g. reading data from a file)
        else:
            (code with interactive features, e.g. manual data input)

        ...

    Returns
    -------
    boolean
        ``True`` - the code is executed in RE Worker environment, otherwise ``False``.
    """
    return os.environ.get(_env_re_worker_active, "false").lower() not in ("", "n", "no", "f", "false", "off", "0")
