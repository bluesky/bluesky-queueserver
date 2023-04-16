import pytest

from bluesky_queueserver.manager.profile_ops import clear_registered_items


@pytest.fixture(autouse=True)
def setup_and_teardown_for_every_test():
    print("Clearing registered items ...")
    clear_registered_items()
    yield
    print("Clearing registered items ...")
    clear_registered_items()
