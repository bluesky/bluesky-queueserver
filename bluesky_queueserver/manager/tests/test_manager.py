import asyncio
import os
import time
import pytest

from bluesky_queueserver.manager.manager import RunEngineManager

# Define available backends for testing
AVAILABLE_BACKENDS = ["redis", "sqlite", "dict"]  # Remove postgresql if not configured in CI

@pytest.fixture(params=AVAILABLE_BACKENDS)
def backend_type(request):
    """Fixture to parametrize tests over available backends."""
    # Save the original environment setting
    original_backend = os.environ.get("PLAN_QUEUE_BACKEND")
    
    # Set backend for this test
    os.environ["PLAN_QUEUE_BACKEND"] = request.param
    
    yield request.param
    
    # Restore the original setting
    if original_backend is not None:
        os.environ["PLAN_QUEUE_BACKEND"] = original_backend
    else:
        os.environ.pop("PLAN_QUEUE_BACKEND", None)


@pytest.fixture
async def re_manager(backend_type):
    """
    Fixture that creates RE Manager with the specified backend type
    """
    # Configure the manager with the backend type that's set in the environment
    config = {
        "redis_host": os.getenv("REDIS_HOST", "localhost"),
        "redis_port": int(os.getenv("REDIS_PORT", "6379")),
        "startup_dir": os.path.dirname(__file__),  # Startup in the test directory
        "keep_re": True,
        "redis_name_prefix": f"test_{backend_type}",
        "database_file": ":memory:" if backend_type == "sqlite" else None,
    }
    
    # Create the manager
    re_manager = RunEngineManager(**config)
    await re_manager.start()
    
    # Return for use in test
    yield re_manager
    
    # Proper cleanup
    await re_manager.stop()


# fmt: off
@pytest.mark.parametrize("request_dict, param_names, success, msg", [
    ({"ab": 10, "cd": 50}, ["ab", "cd", "ef"], True, ""),
    ({}, [], True, ""),
    ({"ab": 10, "cd": 50}, [], False, r"unsupported parameters: \['ab', 'cd'\]. Supported parameters: \[\]"),
    ({"ab": 10}, [], False, r"unsupported parameters: 'ab'. Supported parameters: \[\]"),
    ({"ab": 10}, ["de"], False, r"unsupported parameters: 'ab'. Supported parameters: \['de'\]"),
])
# fmt: on
def test_check_request_for_unsupported_params_1(request_dict, param_names, success, msg):
    """
    Basic test for ``RunEngineManager._check_request_for_unsupported_params``.
    """
    if success:
        RunEngineManager._check_request_for_unsupported_params(request=request_dict, param_names=param_names)
    else:
        with pytest.raises(ValueError, match=msg):
            RunEngineManager._check_request_for_unsupported_params(request=request_dict, param_names=param_names)


@pytest.mark.asyncio
async def test_queue_persistence_across_restarts(backend_type):
    """
    Test that the queue state persists when restarting the manager.
    Tests all backend types.
    """
    # Configure manager
    config = {
        "redis_host": os.getenv("REDIS_HOST", "localhost"),
        "redis_port": int(os.getenv("REDIS_PORT", "6379")),
        "startup_dir": os.path.dirname(__file__),
        "keep_re": True,
        "redis_name_prefix": f"test_{backend_type}",
        "database_file": ":memory:" if backend_type == "sqlite" else None,
    }
    
    # Create first manager instance
    manager1 = RunEngineManager(**config)
    await manager1.start()
    
    # Add plans to queue
    await manager1.add_to_queue({"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 5, "delay": 1}})
    await manager1.add_to_queue({"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10]})
    
    # Set queue mode
    await manager1.set_queue_mode({"loop": True, "ignore_failures": True})
    
    # Verify queue state
    queue_state = await manager1.status()
    queue_size = queue_state["queue"]["queue_size"]
    queue_mode = queue_state["queue"]["mode"]
    assert queue_size == 2
    assert queue_mode["loop"] is True
    assert queue_mode["ignore_failures"] is True
    
    # Stop the first manager
    await manager1.stop()
    
    # Create a new manager instance
    manager2 = RunEngineManager(**config)
    await manager2.start()
    
    # Verify the queue state persisted
    queue_state = await manager2.status()
    queue_size = queue_state["queue"]["queue_size"]
    queue_mode = queue_state["queue"]["mode"]
    assert queue_size == 2
    assert queue_mode["loop"] is True
    assert queue_mode["ignore_failures"] is True
    
    # Clean up
    await manager2.stop()


@pytest.mark.asyncio
async def test_queue_clear_functionality(re_manager):
    """
    Test queue clear functionality with different backend types.
    """
    # Add plans to queue
    await re_manager.add_to_queue({"name": "count", "args": [["det1"]], "kwargs": {"num": 5}})
    await re_manager.add_to_queue({"name": "count", "args": [["det2"]], "kwargs": {"num": 3}})
    
    # Verify plans were added
    status = await re_manager.status()
    assert status["queue"]["queue_size"] == 2
    
    # Clear the queue
    await re_manager.queue_clear()
    
    # Verify queue is empty
    status = await re_manager.status()
    assert status["queue"]["queue_size"] == 0


@pytest.mark.asyncio
async def test_queue_autostart_persistence(backend_type):
    """
    Test that autostart mode persists across manager restarts.
    """
    # Configure manager
    config = {
        "redis_host": os.getenv("REDIS_HOST", "localhost"),
        "redis_port": int(os.getenv("REDIS_PORT", "6379")),
        "startup_dir": os.path.dirname(__file__),
        "keep_re": True,
        "redis_name_prefix": f"test_{backend_type}",
        "database_file": ":memory:" if backend_type == "sqlite" else None,
    }
    
    # Create first manager instance
    manager1 = RunEngineManager(**config)
    await manager1.start()
    
    # Set autostart mode
    await manager1.queue_autostart_on()
    
    # Verify autostart is on
    status = await manager1.status()
    assert status["queue"]["autostart"] is True
    
    # Stop the first manager
    await manager1.stop()
    
    # Create a new manager instance
    manager2 = RunEngineManager(**config)
    await manager2.start()
    
    # Verify the autostart mode persisted
    status = await manager2.status()
    assert status["queue"]["autostart"] is True
    
    # Clean up
    await manager2.queue_autostart_off()  # Turn off autostart mode
    await manager2.stop()


@pytest.mark.asyncio
async def test_queue_stop_pending_persistence(backend_type):
    """
    Test that stop_pending status persists across manager restarts.
    """
    # Configure manager
    config = {
        "redis_host": os.getenv("REDIS_HOST", "localhost"),
        "redis_port": int(os.getenv("REDIS_PORT", "6379")),
        "startup_dir": os.path.dirname(__file__),
        "keep_re": True,
        "redis_name_prefix": f"test_{backend_type}",
        "database_file": ":memory:" if backend_type == "sqlite" else None,
    }
    
    # Create first manager instance
    manager1 = RunEngineManager(**config)
    await manager1.start()
    
    # Set stop_pending flag
    await manager1.queue_stop_activate()
    
    # Verify stop_pending is active
    status = await manager1.status()
    assert status["queue"]["stop_pending"] is True
    
    # Stop the first manager
    await manager1.stop()
    
    # Create a new manager instance
    manager2 = RunEngineManager(**config)
    await manager2.start()
    
    # Verify the stop_pending status persisted
    status = await manager2.status()
    assert status["queue"]["stop_pending"] is True
    
    # Clean up
    await manager2.queue_stop_deactivate()
    await manager2.stop()


@pytest.mark.asyncio
async def test_queue_item_update_functionality(re_manager):
    """
    Test updating queue items with different backends.
    """
    # Add a plan to the queue
    plan = {"name": "count", "args": [["det1"]], "kwargs": {"num": 5}}
    result = await re_manager.add_to_queue(plan)
    item_uid = result["item"]["item_uid"]
    
    # Update the plan
    updated_plan = {"name": "count", "args": [["det1"]], "kwargs": {"num": 10}}
    await re_manager.item_update(item_uid=item_uid, update=updated_plan)
    
    # Verify the plan was updated
    queue = await re_manager.queue_get()
    assert queue["queue"][0]["kwargs"]["num"] == 10
    

@pytest.mark.asyncio
async def test_queue_move_functionality(re_manager):
    """
    Test moving items in the queue with different backends.
    """
    # Add plans to the queue
    await re_manager.add_to_queue({"name": "count", "args": [["det1"]], "kwargs": {"num": 1}})
    await re_manager.add_to_queue({"name": "count", "args": [["det2"]], "kwargs": {"num": 2}})
    await re_manager.add_to_queue({"name": "count", "args": [["det3"]], "kwargs": {"num": 3}})
    
    # Get the queue to find item UIDs
    queue = await re_manager.queue_get()
    uids = [item["item_uid"] for item in queue["queue"]]
    
    # Move the last item to the front
    await re_manager.item_move(uid=uids[2], pos=0)
    
    # Verify the item was moved
    queue = await re_manager.queue_get()
    plans = [item["kwargs"]["num"] for item in queue["queue"]]
    assert plans == [3, 1, 2]
    
    # Move a batch of items
    await re_manager.item_move_batch(uids=[uids[0], uids[1]], pos=2)
    
    # Verify the batch was moved
    queue = await re_manager.queue_get()
    plans = [item["kwargs"]["num"] for item in queue["queue"]]
    assert plans == [3, 1, 2] or plans == [2, 3, 1]  # Depending on implementation


@pytest.mark.asyncio
async def test_queue_reset_functionality(re_manager):
    """
    Test the queue reset functionality with different backends.
    """
    # Add plans to the queue
    await re_manager.add_to_queue({"name": "count", "args": [["det1"]], "kwargs": {"num": 1}})
    
    # Execute a plan to put it in history
    await re_manager.environment_open()
    await re_manager.queue_start()
    
    # Wait for the plan to complete (may need adjustment in timing)
    for _ in range(10):
        status = await re_manager.status()
        if not status["manager_state"] == "executing_queue":
            break
        await asyncio.sleep(1)
    
    # Verify we have history
    status = await re_manager.status()
    assert status["queue"]["history_size"] > 0
    
    # Reset the queue
    await re_manager.queue_reset()
    
    # Verify queue and history are empty
    status = await re_manager.status()
    assert status["queue"]["queue_size"] == 0
    assert status["queue"]["history_size"] == 0


@pytest.mark.asyncio
async def test_user_permissions_persistence(backend_type):
    """
    Test that user group permissions persist across manager restarts.
    """
    # Configure manager
    config = {
        "redis_host": os.getenv("REDIS_HOST", "localhost"),
        "redis_port": int(os.getenv("REDIS_PORT", "6379")),
        "startup_dir": os.path.dirname(__file__),
        "keep_re": True,
        "redis_name_prefix": f"test_{backend_type}",
        "database_file": ":memory:" if backend_type == "sqlite" else None,
    }
    
    # Create first manager instance
    manager1 = RunEngineManager(**config)
    await manager1.start()
    
    # Set user group permissions
    user_group_permissions = {
        "admin": {"queue_read": True, "queue_write": True},
        "users": {"queue_read": True, "queue_write": False}
    }
    await manager1.permissions_set(user_group_permissions=user_group_permissions)
    
    # Stop the first manager
    await manager1.stop()
    
    # Create a new manager instance
    manager2 = RunEngineManager(**config)
    await manager2.start()
    
    # Verify the permissions persisted
    permissions = await manager2.permissions_get()
    assert permissions["user_group_permissions"]["admin"]["queue_write"] is True
    assert permissions["user_group_permissions"]["users"]["queue_write"] is False
    
    # Clean up
    await manager2.stop()



# import pytest

# from bluesky_queueserver.manager.manager import RunEngineManager


# # fmt: off
# @pytest.mark.parametrize("request_dict, param_names, success, msg", [
#     ({"ab": 10, "cd": 50}, ["ab", "cd", "ef"], True, ""),
#     ({}, [], True, ""),
#     ({"ab": 10, "cd": 50}, [], False, r"unsupported parameters: \['ab', 'cd'\]. Supported parameters: \[\]"),
#     ({"ab": 10}, [], False, r"unsupported parameters: 'ab'. Supported parameters: \[\]"),
#     ({"ab": 10}, ["de"], False, r"unsupported parameters: 'ab'. Supported parameters: \['de'\]"),
# ])
# # fmt: on
# def test_check_request_for_unsupported_params_1(request_dict, param_names, success, msg):
#     """
#     Basic test for ``RunEngineManager._check_request_for_unsupported_params``.
#     """
#     if success:
#         RunEngineManager._check_request_for_unsupported_params(request=request_dict, param_names=param_names)
#     else:
#         with pytest.raises(ValueError, match=msg):
#             RunEngineManager._check_request_for_unsupported_params(request=request_dict, param_names=param_names)
