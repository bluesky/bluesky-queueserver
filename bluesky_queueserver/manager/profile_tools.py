import functools
import inspect


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


def load_devices_from_happi(device_names, *, instrument, namespace=None, **kwargs):
    """
    Load the devices from Happi. The devices are specified as a list of device names.
    Since the database may contain devices from multiple instruments (beamlines or endstations),
    the function requires to specify the instrument name, that is included in each search request.
    The instrument name could be arbitrary string, but it must be unique for the instrument
    and consistently used throughout the database (``instrument`` metadata key). If instrument
    name should not be included in the search parameters, then the function should be called with
    ``beamline=None`` set explicitly. The ``instrument`` parameter is set as required to avoid
    potential errors due to accidental loading devices configured for different instruments.

    The function may be called multiple times in a row if needed. For example, if the instrument
    is split into two subsystems ``TEST.1`` and ``TEST.2``, then the devices from both subsystems
    may be loaded into a script as

    .. code-block:: python

        load_devices_from_happi(["det1", "motor1"], instrument="TEST.1", namespace=locals())
        load_devices_from_happi(["det2"], instrument="TEST.2", namespace=locals())

        # Load device associated with any instrument (works as long as the database contains
        #   only one device with this name)
        load_devices_from_happi(["unique_device"], instrument=None, namespace=locals())

    Parameters
    ----------
    device_names : list(str)
        List of device names. It is expected that search will result in one found device per
        device name.
    instrument : str or None
        Instrument (beamline or enstation) name used in database search. This a required parameter.
        Explicitly set ``instrument=None`` to run search without instrument name (e.g. if the
        database contains devices for a single instrument).
    namespace : dict
        Reference to the namespace where the devices should be loaded. It can be the reference
        to ``global()`` or ``locals()`` of the startup script.
    kwargs : dict
        Additional search parameters

    Returns
    -------
    dict
        the dictionary (namespace) that contains loaded the devices. It is not equal to
        ``nspace_ref``. Typically there is no need to use the returned namespace if ``nspace_ref``
        is specified.

    Raises
    ------
    RuntimeError
        Search for one of the device names returns no devices or more than one device.
    """

    kwargs = kwargs or {}

    from happi import Client, load_devices

    client = Client.from_config()

    results = []
    for d_name in device_names:
        # Assemble search parameters
        search_params = dict(kwargs)
        search_params.update({"name": d_name})
        if instrument:
            search_params.update({"instrument": instrument})

        res = client.search(**search_params)
        if not res:
            raise RuntimeError(
                f"No devices with name '{d_name}' were found in Happi database. "
                f"Search parameters {search_params}"
            )
        elif len(res) > 1:
            raise RuntimeError(
                f"Multiple devices with name '{d_name}' were found in Happi database. "
                f"Search parameters {search_params}"
            )
        else:
            # Instantiate the object
            results.append(res[0])

    ns = load_devices(*[_.item for _ in results])
    ns_dict = ns.__dict__
    if namespace:
        namespace.update(ns_dict)
    return ns_dict
