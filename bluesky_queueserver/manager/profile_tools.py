import functools
import inspect


class UserNamespace:
    def __init__(self):
        self.__user_ns = {}

    @property
    def user_ns(self):
        return self.__user_ns

    @user_ns.setter
    def user_ns(self, ns):
        if not isinstance(ns, dict):
            raise TypeError(f"Parameter 'ns' must be of type 'dict': type(ns) is '{type(dict)}'")
        self.__user_ns = ns


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

    The decorated function must have two kwargs: ``user_ns`` - reference
    to RE namespace and ``ipython`` - reference to IPython, which is None
    if the function called not from IPython.
    """

    def get_user_ns():
        from IPython import get_ipython

        ip = get_ipython()
        if ip:
            # Select IPython namespace (only if the code is run from IPython)
            user_ns = ip.user_ns
        else:
            # Select global user namespace that is manually set
            user_ns = global_user_namespace.user_ns
        return ip, user_ns

    if inspect.isgeneratorfunction(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            ip, user_ns = get_user_ns()
            yield from func(*args, user_ns=user_ns, ipython=ip, **kwargs)

    else:

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            ip, user_ns = get_user_ns()
            return func(*args, user_ns=user_ns, ipython=ip, **kwargs)

    params_to_remove = ("user_ns", "ipython")
    sig_params = inspect.signature(wrapper).parameters
    sig = inspect.Signature([sig_params[_] for _ in sig_params if _ not in params_to_remove])
    wrapper.__signature__ = sig

    return wrapper
