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
            yield from func(*args, **kwargs)

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
