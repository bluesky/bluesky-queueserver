import os
import pytest
from bluesky_queueserver.manager.plan_queue_ops import PlanQueueOperations

# Skip all tests in this file if PLAN_QUEUE_BACKEND is set to 'redis'
pytestmark = pytest.mark.skipif(
    os.getenv("PLAN_QUEUE_BACKEND", "redis").lower() == "redis",
    reason="Tests in this file are not applicable to the Redis backend (PLAN_QUEUE_BACKEND='redis')."
)

@pytest.fixture(params=["sqlite", "dict"])
def backend(request):
    """
    Fixture to set up the backend for testing. This will run the tests for both
    the 'sqlite' and 'dict' backends.
    """
    os.environ["PLAN_QUEUE_BACKEND"] = request.param
    yield request.param
    del os.environ["PLAN_QUEUE_BACKEND"]  # Clean up after the test

@pytest.fixture
async def plan_queue_ops(backend):
    """
    Fixture to initialize the PlanQueueOperations instance for the given backend.
    """
    pq_ops = PlanQueueOperations()
    await pq_ops.start()
    yield pq_ops
    await pq_ops.stop()

@pytest.mark.asyncio
async def test_add_and_get_queue(plan_queue_ops):
    """
    Test adding items to the queue and retrieving them.
    """
    # Add an item to the queue
    item = {"name": "example_plan", "args": [1, 2, 3]}
    await plan_queue_ops.add_item_to_queue(item)

    # Verify the queue size
    queue_size = await plan_queue_ops.get_queue_size()
    assert queue_size == 1

    # Retrieve the full queue
    queue = await plan_queue_ops.get_queue_full()
    assert len(queue) == 1
    assert queue[0]["name"] == "example_plan"

@pytest.mark.asyncio
async def test_clear_queue(plan_queue_ops):
    """
    Test clearing the queue.
    """
    # Add items to the queue
    await plan_queue_ops.add_item_to_queue({"name": "plan_1"})
    await plan_queue_ops.add_item_to_queue({"name": "plan_2"})

    # Clear the queue
    await plan_queue_ops.clear_queue()

    # Verify the queue is empty
    queue_size = await plan_queue_ops.get_queue_size()
    assert queue_size == 0

@pytest.mark.asyncio
async def test_plan_queue_mode(plan_queue_ops):
    """
    Test setting and retrieving the plan queue mode.
    """
    # Set the plan queue mode
    mode = {"loop": True, "ignore_failures": False}
    await plan_queue_ops.set_plan_queue_mode(mode)

    # Retrieve the plan queue mode
    retrieved_mode = plan_queue_ops.plan_queue_mode
    assert retrieved_mode["loop"] is True
    assert retrieved_mode["ignore_failures"] is False

@pytest.mark.asyncio
async def test_running_item(plan_queue_ops):
    """
    Test setting and retrieving the currently running item.
    """
    # Add an item to the queue and set it as running
    item = {"name": "running_plan"}
    await plan_queue_ops.add_item_to_queue(item)
    await plan_queue_ops.set_next_item_as_running()

    # Retrieve the running item
    running_item = await plan_queue_ops.get_running_item_info()
    assert running_item["name"] == "running_plan"

    # Mark the running item as completed
    await plan_queue_ops.set_processed_item_as_completed(
        exit_status="completed", run_uids=["uid1"], scan_ids=[1]
    )

    # Verify the running item is cleared
    running_item = await plan_queue_ops.get_running_item_info()
    assert running_item is None

@pytest.mark.asyncio
async def test_user_group_permissions(plan_queue_ops):
    """
    Test saving, retrieving, and clearing user group permissions.
    """
    # Save user group permissions
    permissions = {"admin": ["plan1", "plan2"], "user": ["plan3"]}
    await plan_queue_ops.user_group_permissions_save(permissions)

    # Retrieve the permissions
    retrieved_permissions = await plan_queue_ops.user_group_permissions_retrieve()
    assert retrieved_permissions == permissions

    # Clear the permissions
    await plan_queue_ops.user_group_permissions_clear()

    # Verify the permissions are cleared
    retrieved_permissions = await plan_queue_ops.user_group_permissions_retrieve()
    assert retrieved_permissions is None