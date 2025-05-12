import asyncio
import copy
import json
import os
import pytest
import re
import uuid
from typing import Dict, Optional, Any, List, Union

from bluesky_queueserver.manager.plan_queue_ops import PlanQueueOperations


class PQ:
    """
    The class that creates and initializes an instance of plan queue with SQLite backend for testing.
    Intended for use with ``async with``.
    """

    def __init__(self, db_path=":memory:", name_prefix=None, clear_data=True):
        self._pq = None
        self._db_path = db_path
        self._clear_data = clear_data  # New parameter
        
        # Generate a unique name prefix if none provided
        if name_prefix is None:
            name_prefix = f"qserver_test_{str(uuid.uuid4())[:8]}"
        self._name_prefix = name_prefix

        # Store original environment variable
        self._original_backend = os.environ.get("PLAN_QUEUE_BACKEND")

    async def __aenter__(self):
        """
        Returns initialized instance of plan queue with SQLite backend.
        """
        # Set SQLite as backend
        os.environ["PLAN_QUEUE_BACKEND"] = "sqlite"
        
        self._pq = PlanQueueOperations(
            name_prefix=self._name_prefix,
            sqlite_db_path=self._db_path
        )
        
        await self._pq.start()
        
        # Only clear data if requested
        if self._clear_data:
            await self._pq.clear_queue()
            await self._pq.clear_history()
            await self._pq.user_group_permissions_clear()
            await self._pq.lock_info_clear()
            await self._pq.autostart_mode_clear()
            await self._pq.stop_pending_clear()
        
        return self._pq

    async def __aexit__(self, exc_t, exc_v, exc_tb):
        """
        Cleanup after test.
        """
        # Preserve original environment variable
        if self._original_backend is not None:
            os.environ["PLAN_QUEUE_BACKEND"] = self._original_backend
        else:
            os.environ.pop("PLAN_QUEUE_BACKEND", None)
            
        if self._pq:
            await self._pq.stop()


#1. Initialization and Connection

def test_pq_start_stop():
    """
    Test for the ``PlanQueueOperations.start()`` and ``PlanQueueOperations.stop()``
    with SQLite backend.
    """

    async def testing():
        # Save original env var
        original_backend = os.environ.get("PLAN_QUEUE_BACKEND")
        os.environ["PLAN_QUEUE_BACKEND"] = "sqlite"
        
        try:
            name_prefix = f"qserver_test_{str(uuid.uuid4())[:8]}"
            pq = PlanQueueOperations(name_prefix=name_prefix, sqlite_db_path=":memory:")
            
            await pq.start()
            # Verify we can perform an operation
            assert await pq.get_queue_size() == 0
            
            await pq.stop()
            # Successful stop with no errors is sufficient
        finally:
            # Restore original env var
            if original_backend is not None:
                os.environ["PLAN_QUEUE_BACKEND"] = original_backend
            else:
                os.environ.pop("PLAN_QUEUE_BACKEND", None)

    asyncio.run(testing())


def test_sqlite_name_prefix():
    """
    Test that name_prefix is correctly used for isolating queues.
    """

    async def testing():
        # Create two queues with different prefixes
        async with PQ(name_prefix="prefix1") as pq1:
            async with PQ(name_prefix="prefix2") as pq2:
                
                # Add an item to each queue
                item = {"name": "count", "args": [["det1"]], "kwargs": {"num": 5}}
                await pq1.add_item_to_queue(item)
                await pq2.add_item_to_queue(item)
                
                # Verify each queue has exactly one item
                assert await pq1.get_queue_size() == 1
                assert await pq2.get_queue_size() == 1

    asyncio.run(testing())


def test_sqlite_persistence():
    """
    Test that data persists in a file-backed SQLite database.
    """

    async def testing():
        # Use a temporary file for testing
        db_path = "test_sqlite_queue.db"
        name_prefix = f"qserver_test_{str(uuid.uuid4())[:8]}"

        try:
            # First session - create and populate queue
            async with PQ(db_path=db_path, name_prefix=name_prefix, clear_data=True) as pq:
                # Add items to queue
                item1 = {"name": "count", "args": [["det1"]], "kwargs": {"num": 5}}
                item2 = {"name": "scan", "args": [["det1"], "motor", -1, 1, 10]}
                await pq.add_item_to_queue(item1)
                await pq.add_item_to_queue(item2)

                # Set a plan queue mode
                await pq.set_plan_queue_mode({"loop": True, "ignore_failures": True})

            # Second session - verify data persisted
            async with PQ(db_path=db_path, name_prefix=name_prefix, clear_data=False) as pq2:
                # Verify data persisted
                queue, _ = await pq2.get_queue()
                assert len(queue) == 2
                assert queue[0]["name"] == "count"
                assert queue[1]["name"] == "scan"

                # Verify mode persisted
                mode = pq2.plan_queue_mode
                assert mode["loop"] is True
                assert mode["ignore_failures"] is True

        finally:
            # Clean up file
            if os.path.exists(db_path):
                try:
                    os.unlink(db_path)
                except:
                    pass

    asyncio.run(testing())

# 2. Queue Operations

def test_queue_clean():
    """
    Test cleaning invalid items from the queue.
    """
    
    async def testing():
        async with PQ() as pq:
            # Create a list of items with one invalid item (missing name)
            items = [
                {"name": "count", "args": [["det1"]], "kwargs": {"num": 5}},
                {"args": [["det1"]], "kwargs": {"num": 5}},  # Invalid - missing name
                {"name": "scan", "args": [["det1"], "motor", -1, 1, 10]},
            ]
            
            # Add valid items directly to queue
            for item in items:
                if "name" in item:
                    await pq.add_item_to_queue(item)
            
            # Add the invalid item that should be cleaned during verification
            try:
                await pq.add_item_to_queue({"args": [["det1"]]})
                assert False, "Invalid item should not be added to queue"
            except Exception:
                pass
            
            # Verify only valid items are in the queue
            queue, _ = await pq.get_queue()
            assert len(queue) == 2  # Only the valid items
            
            # Verify item names
            assert queue[0]["name"] == "count"
            assert queue[1]["name"] == "scan"
            
    asyncio.run(testing())


def test_get_queue_full_1():
    """
    Test retrieving complete queue information.
    """
    
    async def testing():
        async with PQ() as pq:
            # Fill queue with sample items
            for _ in range(3):
                item_dict = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 10, "delay": 1}}
                await pq.add_item_to_queue(item_dict)
                
            # Get the queue
            queue, queue_uid = await pq.get_queue()
            
            # Verify queue contains all items
            assert len(queue) == 3
            assert queue_uid is not None
            
            # Verify item structure
            for item in queue:
                assert "name" in item
                assert "args" in item
                assert "kwargs" in item
                assert "item_uid" in item
                assert item["name"] == "count"
                
    asyncio.run(testing())


def test_clear_queue():
    """
    Test clearing the entire queue.
    """
    
    async def testing():
        async with PQ() as pq:
            # Fill queue with sample items
            for _ in range(3):
                item_dict = {"name": "count", "args": [["det1"]]}
                await pq.add_item_to_queue(item_dict)
            
            # Verify queue has items initially
            assert await pq.get_queue_size() == 3
            
            # Clear the queue
            await pq.clear_queue()
            
            # Verify queue is empty
            assert await pq.get_queue_size() == 0
            queue, _ = await pq.get_queue()
            assert len(queue) == 0
    
    asyncio.run(testing())


def test_get_queue_size():
    """
    Test queue size reporting.
    """
    
    async def testing():
        async with PQ() as pq:
            # Queue should be empty initially
            assert await pq.get_queue_size() == 0
            
            # Add items
            for i in range(5):
                item_dict = {"name": f"plan{i}", "args": [[f"det{i}"]]}
                await pq.add_item_to_queue(item_dict)
                
                # Check size increases with each addition
                assert await pq.get_queue_size() == i + 1
            
            # Remove an item and check size decreases
            await pq.pop_item_from_queue(pos=0)
            assert await pq.get_queue_size() == 4
            
            # Clear queue and check size is zero
            await pq.clear_queue()
            assert await pq.get_queue_size() == 0
    
    asyncio.run(testing())


# 3. Item Validation and Filtering

def test_filter_item_parameters():
    """
    Test filtering allowed parameters from items.
    """
    test_cases = [
        ({"name": "plan1", "item_uid": "abcde"}, {"name": "plan1", "item_uid": "abcde"}),
        ({"user": "user1", "user_group": "group1"}, {"user": "user1", "user_group": "group1"}),
        ({"args": [1, 2], "kwargs": {"a": 1, "b": 2}}, {"args": [1, 2], "kwargs": {"a": 1, "b": 2}}),
        ({"item_type": "plan", "meta": {"md1": 1, "md2": 2}}, {"item_type": "plan", "meta": {"md1": 1, "md2": 2}}),
        ({"name": "plan1", "result": {}}, {"name": "plan1"}),  # 'result' should be filtered out
    ]
    
    async def testing():
        async with PQ() as pq:
            for item_in, item_out in test_cases:
                # Use the filter_item_parameters method
                filtered_item = pq.filter_item_parameters(item_in)
                
                # Verify the correct parameters are present
                for key, value in item_out.items():
                    assert key in filtered_item
                    assert filtered_item[key] == value
                
                # Verify filtered parameters are not present
                for key in item_in:
                    if key not in item_out:
                        assert key not in filtered_item
    
    asyncio.run(testing())


def test_verify_item_type():
    """
    Test validation of item type (must be dictionary).
    """
    test_cases = [
        ({"name": "plan1"}, True),
        ("string_value", False),
        (123, False),
        ([1, 2, 3], False),
        (None, False),
    ]
    
    async def testing():
        async with PQ() as pq:
            for item, success in test_cases:
                if success:
                    # Should not raise an exception
                    pq._verify_item_type(item)
                else:
                    # Should raise TypeError
                    with pytest.raises(TypeError):
                        pq._verify_item_type(item)
    
    asyncio.run(testing())


def test_verify_item():
    """
    Test complete item validation with various conditions.
    """
    test_cases = [
        ({"name": "count", "args": [["det1"]]}, True),
        ({"name": "count", "args": [["det1"]], "kwargs": {}}, True),
        ({}, False),  # No name
        ({"args": [["det1"]]}, False),  # No name
        ({"name": ""}, False),  # Empty name
        ({"name": None}, False),  # None name
        ({"name": 123}, False),  # Non-string name
    ]
    
    async def testing():
        async with PQ() as pq:
            for item, success in test_cases:
                if success:
                    # Should not raise an exception
                    await pq._verify_item(item)
                else:
                    # Should raise ValueError
                    with pytest.raises(ValueError):
                        await pq._verify_item(item)
    
    asyncio.run(testing())


def test_clean_item_properties_1():
    """
    Test cleaning of item properties.
    """
    test_cases = [
        # Basic cleanup: empty fields should be set to defaults
        (
            {"name": "count"}, 
            {"name": "count", "args": [], "kwargs": {}, "item_type": "plan", "user": None, "user_group": None}
        ),
        # Custom values should be preserved
        (
            {"name": "count", "args": [1, 2], "kwargs": {"a": 1}, "item_type": "instruction", "user": "user1", "user_group": "admins"},
            {"name": "count", "args": [1, 2], "kwargs": {"a": 1}, "item_type": "instruction", "user": "user1", "user_group": "admins"}
        ),
        # Meta should be initialized if not present
        (
            {"name": "count", "args": []},
            {"name": "count", "args": [], "kwargs": {}, "item_type": "plan", "user": None, "user_group": None}
        ),
    ]
    
    async def testing():
        async with PQ() as pq:
            for item_in, item_out in test_cases:
                # Clean the item
                cleaned_item = await pq._clean_item_properties(copy.deepcopy(item_in))
                
                # Check the required fields were set
                for key, value in item_out.items():
                    assert key in cleaned_item
                    assert cleaned_item[key] == value
                
                # Check format of meta and item_uid
                if "meta" in cleaned_item:
                    assert isinstance(cleaned_item["meta"], dict)
                
                # A UID should have been added
                assert "item_uid" in cleaned_item
                assert isinstance(cleaned_item["item_uid"], str)
                assert len(cleaned_item["item_uid"]) > 0
    
    asyncio.run(testing())

# 4. Item Addition Operations

def test_add_item_to_queue_1():
    """
    Test adding items at different positions within the queue.
    """
    
    async def testing():
        async with PQ() as pq:
            item1 = {"name": "count", "args": [["det1"]], "kwargs": {"num": 1}}
            item2 = {"name": "scan", "args": [["det1"], "motor", -1, 1, 10]}
            item3 = {"name": "rel_scan", "args": [["det1"], "motor", -1, 1, 10]}
            item4 = {"name": "count", "args": [["det2"]], "kwargs": {"num": 5}}
            
            # Add to back (default position)
            await pq.add_item_to_queue(item1)
            
            # Add to front
            await pq.add_item_to_queue(item2, pos="front")
            
            # Add to back explicitly
            await pq.add_item_to_queue(item3, pos="back")
            
            # Add at specific position
            await pq.add_item_to_queue(item4, pos=1)
            
            # Verify queue order: [scan, count(5), count(1), rel_scan]
            queue, _ = await pq.get_queue()
            assert len(queue) == 4
            assert queue[0]["name"] == "scan"
            assert queue[1]["name"] == "count" and queue[1]["kwargs"]["num"] == 5
            assert queue[2]["name"] == "count" and queue[2]["kwargs"]["num"] == 1
            assert queue[3]["name"] == "rel_scan"
    
    asyncio.run(testing())


def test_add_item_to_queue_2():
    """
    Test adding items with reference to other items (before/after).
    """
    
    async def testing():
        async with PQ() as pq:
            # Add initial items
            item1 = {"name": "count", "args": [["det1"]]}
            item2 = {"name": "scan", "args": [["det1"], "motor", -1, 1, 10]}
            item1_added, _ = await pq.add_item_to_queue(item1)
            item2_added, _ = await pq.add_item_to_queue(item2)
            
            # Add items relative to existing ones
            item3 = {"name": "rel_scan", "args": [["det1"], "motor", -1, 1, 10]}
            item4 = {"name": "count", "args": [["det2"]], "kwargs": {"num": 5}}
            
            # Add before the second item
            await pq.add_item_to_queue(item3, before_uid=item2_added["item_uid"])
            
            # Add after the first item
            await pq.add_item_to_queue(item4, after_uid=item1_added["item_uid"])
            
            # Verify queue order: [count(det1), count(det2,5), rel_scan, scan]
            queue, _ = await pq.get_queue()
            assert len(queue) == 4
            assert queue[0]["name"] == "count" and queue[0]["args"][0][0] == "det1"
            assert queue[1]["name"] == "count" and queue[1]["args"][0][0] == "det2"
            assert queue[2]["name"] == "rel_scan"
            assert queue[3]["name"] == "scan"
    
    asyncio.run(testing())


def test_add_item_to_queue_3():
    """
    Test parameter filtering when adding items.
    """
    
    async def testing():
        async with PQ() as pq:
            # Create an item with allowed and filtered parameters
            item_with_extra = {
                "name": "count",
                "args": [["det1"]],
                "kwargs": {"num": 5},
                "result": {"exit_status": "completed"},  # Should be filtered
                "unknown_param": "test",  # Should be filtered
                "user": "test_user"  # Should be kept
            }
            
            # Add the item
            added_item, _ = await pq.add_item_to_queue(item_with_extra)
            
            # Verify allowed parameters are kept
            assert added_item["name"] == "count"
            assert added_item["args"] == [["det1"]]
            assert added_item["kwargs"] == {"num": 5}
            assert added_item["user"] == "test_user"
            
            # Verify filtered parameters are removed
            assert "result" not in added_item
            assert "unknown_param" not in added_item
            
            # Verify the item in the queue has the same structure
            queue, _ = await pq.get_queue()
            assert len(queue) == 1
            assert queue[0]["name"] == "count"
            assert queue[0]["user"] == "test_user"
            assert "result" not in queue[0]
            assert "unknown_param" not in queue[0]
    
    asyncio.run(testing())


def test_add_item_to_queue_4_fail():
    """
    Test failure cases when adding items.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add initial items for referencing
            item1 = {"name": "count", "args": [["det1"]]}
            added_item, _ = await pq.add_item_to_queue(item1)
            
            # Test cases that should fail
            
            # 1. Invalid item (not a dict)
            with pytest.raises(TypeError):
                await pq.add_item_to_queue("not_a_dict")
            
            # 2. Missing required field (name)
            with pytest.raises(ValueError):
                await pq.add_item_to_queue({"args": [["det1"]]})
            
            # 3. Invalid position specification
            with pytest.raises(ValueError):
                await pq.add_item_to_queue({"name": "count"}, pos="middle")
            
            # 4. Invalid index position
            with pytest.raises(IndexError):
                await pq.add_item_to_queue({"name": "count"}, pos=10)
            
            # 5. Both position and uid specified
            with pytest.raises(ValueError):
                await pq.add_item_to_queue(
                    {"name": "count"},
                    pos="back",
                    before_uid="uid"
                )
            
            # 6. Non-existent uid
            with pytest.raises(RuntimeError):
                await pq.add_item_to_queue(
                    {"name": "count"},
                    before_uid="non_existent_uid"
                )
    
    asyncio.run(testing())


def test_add_batch_to_queue_1():
    """
    Test batch addition of items with different positions.
    """
    
    async def testing():
        async with PQ() as pq:
            items = [
                {"name": "count", "args": [["det1"]], "kwargs": {"num": 1}},
                {"name": "scan", "args": [["det1"], "motor", -1, 1, 10]},
                {"name": "rel_scan", "args": [["det1"], "motor", -1, 1, 10]},
            ]
            
            # Add batch to back (default)
            items_added, results, queue_size, success = await pq.add_batch_to_queue(items)
            assert success is True
            assert queue_size == 3
            assert len(items_added) == 3
            
            # Verify queue order
            queue, _ = await pq.get_queue()
            assert len(queue) == 3
            assert queue[0]["name"] == "count"
            assert queue[1]["name"] == "scan"
            assert queue[2]["name"] == "rel_scan"
            
            # Clear queue
            await pq.clear_queue()
            
            # Add batch to front
            items_added, results, queue_size, success = await pq.add_batch_to_queue(items, pos="front")
            assert success is True
            assert queue_size == 3
            
            # Verify queue order (should be reversed from insertion order)
            queue, _ = await pq.get_queue()
            assert len(queue) == 3
            assert queue[0]["name"] == "rel_scan"
            assert queue[1]["name"] == "scan"
            assert queue[2]["name"] == "count"
    
    asyncio.run(testing())


def test_add_batch_to_queue_2():
    """
    Test parameter filtering in batch operations.
    """
    
    async def testing():
        async with PQ() as pq:
            items = [
                {
                    "name": "count",
                    "args": [["det1"]],
                    "kwargs": {"num": 1},
                    "result": {"exit_status": "completed"},  # Should be filtered
                    "user": "test_user"  # Should be kept
                },
                {
                    "name": "scan",
                    "args": [["det1"], "motor", -1, 1, 10],
                    "unknown_param": "test"  # Should be filtered
                }
            ]
            
            # Add batch
            items_added, results, queue_size, success = await pq.add_batch_to_queue(items)
            assert success is True
            assert queue_size == 2
            
            # Verify filtering
            for item in items_added:
                assert "result" not in item
                assert "unknown_param" not in item
            
            # Check first item kept user
            assert items_added[0]["user"] == "test_user"
            
            # Verify the queue has the same structure
            queue, _ = await pq.get_queue()
            assert "result" not in queue[0]
            assert "unknown_param" not in queue[1]
    
    asyncio.run(testing())


def test_add_batch_to_queue_3_fail():
    """
    Test failure cases in batch operations.
    """
    
    async def testing():
        async with PQ() as pq:
            # Valid items for mixed tests
            valid_items = [
                {"name": "count", "args": [["det1"]]},
                {"name": "scan", "args": [["det1"], "motor", -1, 1, 10]},
            ]
            
            # 1. Empty batch
            items_added, results, queue_size, success = await pq.add_batch_to_queue([])
            assert success is True
            assert queue_size == 0
            assert len(items_added) == 0
            
            # 2. Invalid items in batch
            invalid_batch = [
                {"name": "count"},  # Valid
                "not_a_dict",       # Invalid
                {"args": [["det1"]]}  # Invalid (missing name)
            ]
            
            items_added, results, queue_size, success = await pq.add_batch_to_queue(invalid_batch)
            assert success is False
            assert queue_size == 1  # Only the first valid item added
            assert len(items_added) == 1
            
            # 3. Invalid position
            with pytest.raises(ValueError):
                await pq.add_batch_to_queue(valid_items, pos="middle")
            
            # 4. Invalid index position
            with pytest.raises(IndexError):
                await pq.add_batch_to_queue(valid_items, pos=10)
    
    asyncio.run(testing())

# 5. Item Retrieval and Removal

def test_get_item_1():
    """
    Test retrieving items by position or UID.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add items
            items = [
                {"name": "count", "args": [["det1"]], "kwargs": {"num": 1}},
                {"name": "scan", "args": [["det1"], "motor", -1, 1, 10]},
                {"name": "rel_scan", "args": [["det1"], "motor", -1, 1, 10]},
            ]
            
            for item in items:
                await pq.add_item_to_queue(item)
            
            # Get queue to find UIDs
            queue, _ = await pq.get_queue()
            uid_list = [item["item_uid"] for item in queue]
            
            # Get by position - front
            item_front = await pq.get_item(pos="front")
            assert item_front["name"] == "count"
            
            # Get by position - back
            item_back = await pq.get_item(pos="back")
            assert item_back["name"] == "rel_scan"
            
            # Get by position - index
            item_middle = await pq.get_item(pos=1)
            assert item_middle["name"] == "scan"
            
            # Get by UID
            item_by_uid = await pq.get_item(uid=uid_list[1])
            assert item_by_uid["name"] == "scan"
            assert item_by_uid["item_uid"] == uid_list[1]
    
    asyncio.run(testing())


def test_get_item_2_fail():
    """
    Test failures when retrieving items.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add an item
            item = {"name": "count", "args": [["det1"]]}
            await pq.add_item_to_queue(item)
            
            # No parameters
            with pytest.raises(ValueError):
                await pq.get_item()
            
            # Both parameters
            with pytest.raises(ValueError):
                await pq.get_item(pos=0, uid="some_uid")
            
            # Invalid position type
            with pytest.raises(ValueError):
                await pq.get_item(pos="middle")
            
            # Position out of range
            with pytest.raises(IndexError):
                await pq.get_item(pos=10)
            
            # Non-existent UID
            with pytest.raises(RuntimeError):
                await pq.get_item(uid="non_existent_uid")
            
            # Empty queue
            await pq.clear_queue()
            with pytest.raises(IndexError):
                await pq.get_item(pos=0)
    
    asyncio.run(testing())


def test_pop_item_from_queue_1():
    """
    Test removing items from specified positions.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add items
            items = [
                {"name": "count", "args": [["det1"]], "kwargs": {"num": 1}},
                {"name": "scan", "args": [["det1"], "motor", -1, 1, 10]},
                {"name": "rel_scan", "args": [["det1"], "motor", -1, 1, 10]},
            ]
            
            for item in items:
                await pq.add_item_to_queue(item)
            
            # Verify queue size
            assert await pq.get_queue_size() == 3
            
            # Remove from front
            popped_item, new_size = await pq.pop_item_from_queue(pos="front")
            assert popped_item["name"] == "count"
            assert new_size == 2
            
            # Verify queue state
            queue, _ = await pq.get_queue()
            assert len(queue) == 2
            assert queue[0]["name"] == "scan"
            assert queue[1]["name"] == "rel_scan"
            
            # Remove from specific position
            popped_item, new_size = await pq.pop_item_from_queue(pos=1)
            assert popped_item["name"] == "rel_scan"
            assert new_size == 1
            
            # Verify queue state
            queue, _ = await pq.get_queue()
            assert len(queue) == 1
            assert queue[0]["name"] == "scan"
    
    asyncio.run(testing())


def test_pop_item_from_queue_2():
    """
    Test removing from empty queue.
    """
    
    async def testing():
        async with PQ() as pq:
            # Ensure queue is empty
            await pq.clear_queue()
            assert await pq.get_queue_size() == 0
            
            # Try to remove from empty queue
            with pytest.raises(IndexError):
                await pq.pop_item_from_queue(pos="front")
    
    asyncio.run(testing())


def test_pop_item_from_queue_3():
    """
    Test removing items by UID.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add items
            items = [
                {"name": "count", "args": [["det1"]], "kwargs": {"num": 1}},
                {"name": "scan", "args": [["det1"], "motor", -1, 1, 10]},
                {"name": "rel_scan", "args": [["det1"], "motor", -1, 1, 10]},
            ]
            
            for item in items:
                await pq.add_item_to_queue(item)
            
            # Get queue to find UIDs
            queue, _ = await pq.get_queue()
            middle_uid = queue[1]["item_uid"]
            
            # Remove by UID
            popped_item, new_size = await pq.pop_item_from_queue(uid=middle_uid)
            assert popped_item["name"] == "scan"
            assert new_size == 2
            
            # Verify queue state
            queue, _ = await pq.get_queue()
            assert len(queue) == 2
            assert queue[0]["name"] == "count"
            assert queue[1]["name"] == "rel_scan"
    
    asyncio.run(testing())


def test_pop_item_from_queue_4_fail():
    """
    Test failures in removing items.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add items
            item = {"name": "count", "args": [["det1"]]}
            await pq.add_item_to_queue(item)
            
            # No parameters
            with pytest.raises(ValueError):
                await pq.pop_item_from_queue()
            
            # Both parameters
            with pytest.raises(ValueError):
                await pq.pop_item_from_queue(pos=0, uid="some_uid")
            
            # Invalid position type
            with pytest.raises(ValueError):
                await pq.pop_item_from_queue(pos="middle")
            
            # Position out of range
            with pytest.raises(IndexError):
                await pq.pop_item_from_queue(pos=10)
            
            # Non-existent UID
            with pytest.raises(RuntimeError):
                await pq.pop_item_from_queue(uid="non_existent_uid")
    
    asyncio.run(testing())


def test_pop_items_from_queue_batch_1():
    """
    Test batch removal operations.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add items
            items = [
                {"name": "count", "args": [["det1"]], "kwargs": {"num": 1}},
                {"name": "scan", "args": [["det1"], "motor", -1, 1, 10]},
                {"name": "rel_scan", "args": [["det1"], "motor", -1, 1, 10]},
                {"name": "count", "args": [["det2"]], "kwargs": {"num": 5}},
            ]
            
            for item in items:
                await pq.add_item_to_queue(item)
            
            # Get queue to find UIDs
            queue, _ = await pq.get_queue()
            uid_list = [item["item_uid"] for item in queue]
            
            # Remove batch by UIDs (first and third items)
            uids_to_remove = [uid_list[0], uid_list[2]]
            removed_items, new_size = await pq.pop_item_from_queue_batch(uids=uids_to_remove)
            
            # Verify batch removal
            assert len(removed_items) == 2
            assert removed_items[0]["name"] == "count" and removed_items[0]["kwargs"]["num"] == 1
            assert removed_items[1]["name"] == "rel_scan"
            assert new_size == 2
            
            # Verify queue state
            queue, _ = await pq.get_queue()
            assert len(queue) == 2
            assert queue[0]["name"] == "scan"
            assert queue[1]["name"] == "count" and queue[1]["kwargs"]["num"] == 5
            
            # Test empty list of UIDs
            removed_items, new_size = await pq.pop_item_from_queue_batch(uids=[])
            assert len(removed_items) == 0
            assert new_size == 2
            
            # Test non-existent UIDs
            with pytest.raises(RuntimeError):
                await pq.pop_item_from_queue_batch(uids=["non_existent_uid"])
    
    asyncio.run(testing())

# 6. Item Modification

def test_replace_item_1():
    """
    Test replacing items with/without changing UIDs.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add initial items
            item1 = {"name": "count", "args": [["det1"]]}
            item2 = {"name": "scan", "args": [["det1"], "motor", -1, 1, 10]}
            
            item1_added, _ = await pq.add_item_to_queue(item1)
            item2_added, _ = await pq.add_item_to_queue(item2)
            
            # Create replacement item with same UID
            replacement1 = {
                "name": "count", 
                "args": [["det1", "det2"]], 
                "kwargs": {"num": 5},
                "item_uid": item1_added["item_uid"]
            }
            
            # Create replacement item without UID (new one will be generated)
            replacement2 = {
                "name": "rel_scan", 
                "args": [["det1"], "motor", -1, 1, 5]
            }
            
            # Replace first item keeping UID
            old_item1, queue_size = await pq.replace_item(replacement1, item_uid=item1_added["item_uid"])
            
            # Replace second item generating new UID
            old_item2, queue_size = await pq.replace_item(replacement2, item_pos=1)
            
            # Verify queue
            queue, _ = await pq.get_queue()
            assert len(queue) == 2
            
            # First item should have same UID but updated content
            assert queue[0]["name"] == "count"
            assert queue[0]["args"] == [["det1", "det2"]]
            assert queue[0]["kwargs"]["num"] == 5
            assert queue[0]["item_uid"] == item1_added["item_uid"]
            
            # Second item should have new content and new UID
            assert queue[1]["name"] == "rel_scan"
            assert queue[1]["args"] == [["det1"], "motor", -1, 1, 5]
            assert queue[1]["item_uid"] != item2_added["item_uid"]
    
    asyncio.run(testing())


def test_replace_item_2():
    """
    Test replacing items without UIDs.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add initial items
            item1 = {"name": "count", "args": [["det1"]]}
            item1_added, _ = await pq.add_item_to_queue(item1)
            
            # Create replacement without UID
            replacement = {"name": "scan", "args": [["det1"], "motor", -1, 1, 10]}
            
            # Replace by position
            old_item, queue_size = await pq.replace_item(replacement, item_pos=0)
            
            # Verify original item returned
            assert old_item["name"] == "count"
            assert old_item["item_uid"] == item1_added["item_uid"]
            
            # Verify queue
            queue, _ = await pq.get_queue()
            assert len(queue) == 1
            assert queue[0]["name"] == "scan"
            assert queue[0]["args"] == [["det1"], "motor", -1, 1, 10]
            assert "item_uid" in queue[0]  # Should have a new UID
            assert queue[0]["item_uid"] != item1_added["item_uid"]
    
    asyncio.run(testing())


def test_replace_item_3():
    """
    Test parameter filtering during replacement.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add initial item
            item = {"name": "count", "args": [["det1"]]}
            item_added, _ = await pq.add_item_to_queue(item)
            
            # Create replacement with allowed and filtered parameters
            replacement = {
                "name": "scan", 
                "args": [["det1"], "motor", -1, 1, 10],
                "kwargs": {"num": 5},
                "user": "test_user",
                "result": {"exit_status": "completed"},  # Should be filtered
                "unknown_param": "test"  # Should be filtered
            }
            
            # Replace item
            old_item, queue_size = await pq.replace_item(replacement, item_uid=item_added["item_uid"])
            
            # Verify queue
            queue, _ = await pq.get_queue()
            assert len(queue) == 1
            
            # Verify allowed parameters are kept
            assert queue[0]["name"] == "scan"
            assert queue[0]["args"] == [["det1"], "motor", -1, 1, 10]
            assert queue[0]["kwargs"]["num"] == 5
            assert queue[0]["user"] == "test_user"
            
            # Verify filtered parameters are removed
            assert "result" not in queue[0]
            assert "unknown_param" not in queue[0]
    
    asyncio.run(testing())


def test_replace_item_4_failing():
    """
    Test failure cases for item replacement.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add initial item
            item = {"name": "count", "args": [["det1"]]}
            item_added, _ = await pq.add_item_to_queue(item)
            
            # Test cases that should fail
            
            # 1. Both pos and uid specified
            with pytest.raises(ValueError):
                await pq.replace_item({"name": "scan"}, item_pos=0, item_uid=item_added["item_uid"])
            
            # 2. Neither pos nor uid specified
            with pytest.raises(ValueError):
                await pq.replace_item({"name": "scan"})
            
            # 3. Invalid position
            with pytest.raises(IndexError):
                await pq.replace_item({"name": "scan"}, item_pos=10)
            
            # 4. Non-existent UID
            with pytest.raises(RuntimeError):
                await pq.replace_item({"name": "scan"}, item_uid="non_existent_uid")
            
            # 5. Invalid item (not a dict)
            with pytest.raises(TypeError):
                await pq.replace_item("not_a_dict", item_pos=0)
            
            # 6. Invalid item (missing name)
            with pytest.raises(ValueError):
                await pq.replace_item({"args": [["det1"]]}, item_pos=0)
    
    asyncio.run(testing())


def test_move_item_1():
    """
    Test moving items to various positions.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add items
            items = [
                {"name": "count1", "args": [["det1"]]},
                {"name": "count2", "args": [["det2"]]},
                {"name": "count3", "args": [["det3"]]},
                {"name": "count4", "args": [["det4"]]},
            ]
            
            for item in items:
                await pq.add_item_to_queue(item)
            
            # Verify initial queue order
            queue, _ = await pq.get_queue()
            assert [item["name"] for item in queue] == ["count1", "count2", "count3", "count4"]
            
            # Move item from front to back
            moved_item, queue_size = await pq.move_item(pos=0, pos_dest="back")
            
            # Verify queue order
            queue, _ = await pq.get_queue()
            assert [item["name"] for item in queue] == ["count2", "count3", "count4", "count1"]
            
            # Move item from back to front
            moved_item, queue_size = await pq.move_item(pos="back", pos_dest="front")
            
            # Verify queue order
            queue, _ = await pq.get_queue()
            assert [item["name"] for item in queue] == ["count1", "count2", "count3", "count4"]
            
            # Move item from middle to specific position
            moved_item, queue_size = await pq.move_item(pos=1, pos_dest=3)
            
            # Verify queue order
            queue, _ = await pq.get_queue()
            assert [item["name"] for item in queue] == ["count1", "count3", "count4", "count2"]
            
            # Move item by UID
            middle_item_uid = queue[1]["item_uid"]
            moved_item, queue_size = await pq.move_item(uid=middle_item_uid, pos_dest="front")
            
            # Verify queue order
            queue, _ = await pq.get_queue()
            assert [item["name"] for item in queue] == ["count3", "count1", "count4", "count2"]
    
    asyncio.run(testing())


def test_move_item_2():
    """
    Test preserving item data when moving.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add complex item with metadata
            item = {
                "name": "complex_plan",
                "args": [["det1"]],
                "kwargs": {"num": 5, "delay": 1},
                "user": "test_user",
                "user_group": "test_group",
                "meta": {"importance": "high", "sample": "A123"}
            }
            
            item_added, _ = await pq.add_item_to_queue(item)
            
            # Add another item
            await pq.add_item_to_queue({"name": "simple_plan"})
            
            # Move the complex item
            moved_item, _ = await pq.move_item(pos=0, pos_dest=1)
            
            # Verify all data preserved
            queue, _ = await pq.get_queue()
            moved_queue_item = queue[1]
            
            assert moved_queue_item["name"] == "complex_plan"
            assert moved_queue_item["args"] == [["det1"]]
            assert moved_queue_item["kwargs"]["num"] == 5
            assert moved_queue_item["kwargs"]["delay"] == 1
            assert moved_queue_item["user"] == "test_user"
            assert moved_queue_item["user_group"] == "test_group"
            assert moved_queue_item["meta"]["importance"] == "high"
            assert moved_queue_item["meta"]["sample"] == "A123"
            assert moved_queue_item["item_uid"] == item_added["item_uid"]
    
    asyncio.run(testing())


def test_move_batch_1():
    """
    Test moving multiple items at once.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add items
            items = [
                {"name": "plan1"},
                {"name": "plan2"},
                {"name": "plan3"},
                {"name": "plan4"},
                {"name": "plan5"},
            ]
            
            for item in items:
                await pq.add_item_to_queue(item)
            
            # Get UIDs
            queue, _ = await pq.get_queue()
            uids = [item["item_uid"] for item in queue]
            
            # Move multiple items to front (plans 1, 3, 5)
            uids_to_move = [uids[0], uids[2], uids[4]]
            moved_items, queue_size = await pq.move_batch(uids=uids_to_move, pos_dest="front")
            
            # Verify moved items
            assert len(moved_items) == 3
            assert [item["name"] for item in moved_items] == ["plan1", "plan3", "plan5"]
            
            # Verify queue order: first the moved items (5, 3, 1) then the rest (2, 4)
            queue, _ = await pq.get_queue()
            assert [item["name"] for item in queue] == ["plan5", "plan3", "plan1", "plan2", "plan4"]
            
            # Move multiple items to back
            uids_to_move = [uids[0], uids[2]]  # plan1, plan3
            moved_items, queue_size = await pq.move_batch(uids=uids_to_move, pos_dest="back")
            
            # Verify queue order: plan5, plan2, plan4, plan3, plan1
            queue, _ = await pq.get_queue()
            assert [item["name"] for item in queue] == ["plan5", "plan2", "plan4", "plan3", "plan1"]
            
            # Move multiple items to specific position
            uids_to_move = [uids[0], uids[4]]  # plan1, plan5
            moved_items, queue_size = await pq.move_batch(uids=uids_to_move, pos_dest=1)
            
            # Verify queue order: plan2, plan5, plan1, plan4, plan3
            queue, _ = await pq.get_queue()
            assert [item["name"] for item in queue] == ["plan2", "plan5", "plan1", "plan4", "plan3"]
            
            # Test error cases
            
            # Empty list of UIDs
            moved_items, queue_size = await pq.move_batch(uids=[], pos_dest="front")
            assert len(moved_items) == 0
            
            # Non-existent UID
            with pytest.raises(RuntimeError):
                await pq.move_batch(uids=["non_existent_uid"], pos_dest="front")
            
            # Invalid position
            with pytest.raises(ValueError):
                await pq.move_batch(uids=[uids[0]], pos_dest="middle")
    
    asyncio.run(testing())

# 7. UID Management

def test_new_item_uid():
    """
    Test UID generation.
    """
    
    async def testing():
        async with PQ() as pq:
            # Generate multiple UIDs
            uid1 = pq.new_item_uid()
            uid2 = pq.new_item_uid()
            uid3 = pq.new_item_uid()
            
            # Verify UIDs are valid strings
            assert isinstance(uid1, str)
            assert isinstance(uid2, str)
            assert isinstance(uid3, str)
            
            # Verify UIDs are unique
            assert uid1 != uid2
            assert uid1 != uid3
            assert uid2 != uid3
            
            # Verify UIDs have proper length (should be uuid-like)
            assert len(uid1) > 8
    
    asyncio.run(testing())


def test_set_new_item_uuid():
    """
    Test setting new UIDs on items.
    """
    
    async def testing():
        async with PQ() as pq:
            # Create items without UIDs
            item1 = {"name": "plan1"}
            item2 = {"name": "plan2"}
            
            # Set UIDs
            item1_with_uid = await pq.set_new_item_uuid(item1)
            item2_with_uid = await pq.set_new_item_uuid(item2)
            
            # Verify UIDs are added
            assert "item_uid" in item1_with_uid
            assert "item_uid" in item2_with_uid
            
            # Verify UIDs are unique
            assert item1_with_uid["item_uid"] != item2_with_uid["item_uid"]
            
            # Verify original item doesn't change if it already has a UID
            item3 = {"name": "plan3", "item_uid": "existing-uid"}
            item3_result = await pq.set_new_item_uuid(item3)
            assert item3_result["item_uid"] == "existing-uid"
    
    asyncio.run(testing())


def test_get_index_by_uid_1():
    """
    Test finding items by UID.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add items
            items = [
                {"name": "plan1"},
                {"name": "plan2"},
                {"name": "plan3"},
            ]
            
            for item in items:
                await pq.add_item_to_queue(item)
            
            # Get UIDs
            queue, _ = await pq.get_queue()
            uids = [item["item_uid"] for item in queue]
            
            # Find positions by UID
            pos0 = await pq._get_index_by_uid(uids[0])
            pos1 = await pq._get_index_by_uid(uids[1])
            pos2 = await pq._get_index_by_uid(uids[2])
            
            # Verify correct positions
            assert pos0 == 0
            assert pos1 == 1
            assert pos2 == 2
            
            # Test non-existent UID
            with pytest.raises(RuntimeError):
                await pq._get_index_by_uid("non_existent_uid")
    
    asyncio.run(testing())


def test_get_index_by_uid_batch_1():
    """
    Test batch operations with UIDs.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add items
            items = [
                {"name": "plan1"},
                {"name": "plan2"},
                {"name": "plan3"},
                {"name": "plan4"},
            ]
            
            for item in items:
                await pq.add_item_to_queue(item)
            
            # Get UIDs
            queue, _ = await pq.get_queue()
            uids = [item["item_uid"] for item in queue]
            
            # Get positions for multiple UIDs
            positions = await pq._get_index_by_uid_batch([uids[1], uids[3], uids[0]])
            
            # Verify positions
            assert positions == [1, 3, 0]
            
            # Test with non-existent UID
            with pytest.raises(RuntimeError):
                await pq._get_index_by_uid_batch([uids[0], "non_existent_uid"])
            
            # Test with empty list
            positions = await pq._get_index_by_uid_batch([])
            assert positions == []
    
    asyncio.run(testing())


def test_uid_dict_1():
    """
    Test basic UID dictionary operations.
    """
    
    async def testing():
        async with PQ() as pq:
            # Initially empty
            uid_dict = pq._uid_dict
            
            # Add an item to the queue
            item = {"name": "test_plan"}
            added_item, _ = await pq.add_item_to_queue(item)
            uid = added_item["item_uid"]
            
            # Verify UID is in dictionary after adding
            assert uid in pq._uid_dict
            
            # Remove the item
            await pq.pop_item_from_queue(pos=0)
            
            # Verify UID is removed from dictionary
            assert uid not in pq._uid_dict
    
    asyncio.run(testing())


def test_uid_dict_2():
    """
    Test UID dictionary updates and plan_queue_uid changes.
    """
    
    async def testing():
        async with PQ() as pq:
            # Get initial plan_queue_uid
            initial_uid = pq.plan_queue_uid
            
            # Add an item - should change queue UID
            await pq.add_item_to_queue({"name": "plan1"})
            first_update_uid = pq.plan_queue_uid
            assert initial_uid != first_update_uid
            
            # Modify queue - should change queue UID again
            await pq.add_item_to_queue({"name": "plan2"})
            second_update_uid = pq.plan_queue_uid
            assert first_update_uid != second_update_uid
            
            # Get UIDs
            queue, _ = await pq.get_queue()
            item_uids = [item["item_uid"] for item in queue]
            
            # Verify all item UIDs are in the dictionary
            for uid in item_uids:
                assert uid in pq._uid_dict
    
    asyncio.run(testing())


def test_uid_dict_3_initialize():
    """
    Test UID dictionary initialization.
    """
    
    async def testing():
        # Use a temporary file for testing
        db_path = "test_sqlite_queue.db"
        name_prefix = f"qserver_test_{str(uuid.uuid4())[:8]}"
        
        try:
            # First session - create and populate queue
            async with PQ(db_path=db_path, name_prefix=name_prefix) as pq:
                # Add items to queue
                await pq.add_item_to_queue({"name": "plan1"})
                await pq.add_item_to_queue({"name": "plan2"})
                
                # Get UIDs before closing
                queue1, _ = await pq.get_queue()
                uids1 = [item["item_uid"] for item in queue1]
            
            # Second session - create new connection with same file
            async with PQ(db_path=db_path, name_prefix=name_prefix) as pq2:
                # Get queue from new connection
                queue2, _ = await pq2.get_queue()
                uids2 = [item["item_uid"] for item in queue2]
                
                # Verify UIDs are the same (preserved from file)
                assert uids1 == uids2
                
                # Verify UID dictionary was initialized
                for uid in uids2:
                    assert uid in pq2._uid_dict
                
        finally:
            # Clean up file
            if os.path.exists(db_path):
                try:
                    os.unlink(db_path)
                except:
                    pass
    
    asyncio.run(testing())


def test_uid_dict_4_failing():
    """
    Test error handling for UID operations.
    """
    
    async def testing():
        async with PQ() as pq:
            # Test duplicate UID
            await pq.add_item_to_queue({"name": "plan1"})
            
            queue, _ = await pq.get_queue()
            uid = queue[0]["item_uid"]
            
            # Create item with duplicate UID
            item_with_dup_uid = {"name": "plan2", "item_uid": uid}
            
            # Adding item with existing UID should fail
            with pytest.raises(RuntimeError):
                await pq.add_item_to_queue(item_with_dup_uid)
            
            # Create replacement with duplicate UID for another item
            await pq.add_item_to_queue({"name": "plan3"})
            
            # Try replacing second item with first item's UID
            with pytest.raises(RuntimeError):
                await pq.replace_item({"name": "replacement", "item_uid": uid}, item_pos=1)
    
    asyncio.run(testing())

# 8. Plan Execution Flow

def test_running_plan_info():
    """
    Test running plan status methods.
    """
    
    async def testing():
        async with PQ() as pq:
            # Initially no running plan
            assert await pq.is_item_running() is False
            assert await pq.get_running_item_info() == {}
            
            # Add items to queue
            item1 = {"name": "count", "args": [["det1"]], "kwargs": {"num": 5}}
            await pq.add_item_to_queue(item1)
            
            # Set item as running
            running_item = await pq.set_next_item_as_running()
            
            # Check running status
            assert await pq.is_item_running() is True
            
            # Get running item info
            running_info = await pq.get_running_item_info()
            assert running_info["name"] == "count"
            assert running_info["args"] == [["det1"]]
            assert running_info["kwargs"]["num"] == 5
            assert "item_uid" in running_info
            
            # Complete item
            await pq.set_processed_item_as_completed(exit_status="completed")
            
            # Check running status again
            assert await pq.is_item_running() is False
            assert await pq.get_running_item_info() == {}
    
    asyncio.run(testing())


def test_process_next_item_1():
    """
    Test getting next items for execution.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add items
            items = [
                {"name": "count", "args": [["det1"]], "kwargs": {"num": 5}},
                {"name": "scan", "args": [["det1"], "motor", -1, 1, 10]},
            ]
            
            for item in items:
                await pq.add_item_to_queue(item)
            
            # Process next item
            item_processed = await pq.process_next_item()
            
            # Verify the processed item
            assert item_processed["name"] == "count"
            assert item_processed["args"] == [["det1"]]
            assert item_processed["kwargs"]["num"] == 5
            
            # Verify queue size decreased
            assert await pq.get_queue_size() == 1
            
            # Verify item is running
            assert await pq.is_item_running() is True
            running_item = await pq.get_running_item_info()
            assert running_item["name"] == "count"
            
            # Process next item should raise an exception (already running)
            with pytest.raises(RuntimeError):
                await pq.process_next_item()
    
    asyncio.run(testing())


def test_process_next_item_2():
    """
    Tests processing instruction items.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add an instruction item
            instruction = {
                "name": "queue_stop",
                "item_type": "instruction",
                "args": [],
                "kwargs": {}
            }
            
            await pq.add_item_to_queue(instruction)
            
            # Process instruction
            item_processed = await pq.process_next_item()
            
            # Verify processed item
            assert item_processed["name"] == "queue_stop"
            assert item_processed["item_type"] == "instruction"
            
            # Check that instructions are processed and not left running
            assert await pq.is_item_running() is True
            
            # Complete the instruction
            await pq.set_processed_item_as_completed(exit_status="completed")
            
            # Verify it's added to history
            history, _ = await pq.get_history()
            assert len(history) == 1
            assert history[0]["name"] == "queue_stop"
            assert history[0]["item_type"] == "instruction"
    
    asyncio.run(testing())


def test_process_next_item_3():
    """
    Tests UID assignments during processing.
    """
    
    async def testing():
        async with PQ() as pq:
            # Create an item without UID
            item = {"name": "count", "args": [["det1"]]}
            
            # Process the item directly (not from queue)
            processed_item = await pq.process_next_item(item=item)
            
            # Verify UID was assigned
            assert "item_uid" in processed_item
            assert isinstance(processed_item["item_uid"], str)
            assert len(processed_item["item_uid"]) > 8
            
            # Create an item with existing UID
            item_with_uid = {"name": "scan", "args": [["det1"]], "item_uid": "test-uid-123"}
            
            # Complete current item
            await pq.set_processed_item_as_completed(exit_status="completed")
            
            # Process the item with existing UID
            processed_item = await pq.process_next_item(item=item_with_uid)
            
            # Verify UID was preserved
            assert processed_item["item_uid"] == "test-uid-123"
    
    asyncio.run(testing())


def test_process_next_item_4_fail():
    """
    Tests failures during item processing.
    """
    
    async def testing():
        async with PQ() as pq:
            # Test empty queue
            with pytest.raises(RuntimeError, match="Queue is empty"):
                await pq.process_next_item()
            
            # Test invalid item
            with pytest.raises(TypeError):
                await pq.process_next_item(item="not_a_dict")
            
            # Test item missing required field (name)
            with pytest.raises(ValueError):
                await pq.process_next_item(item={"args": []})
            
            # Add an item and set it as running
            await pq.add_item_to_queue({"name": "count", "args": [["det1"]]})
            await pq.set_next_item_as_running()
            
            # Try to process another item while one is running
            with pytest.raises(RuntimeError, match="Another plan is running"):
                await pq.process_next_item(item={"name": "scan"})
    
    asyncio.run(testing())


def test_set_processed_item_as_completed_1():
    """
    Tests completing running plans.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add and process an item
            await pq.add_item_to_queue({"name": "count", "args": [["det1"]]})
            await pq.set_next_item_as_running()
            
            # Verify we have a running item
            assert await pq.is_item_running() is True
            
            # Mark as completed with run data
            exit_status = "completed"
            run_uids = ["abc123", "def456"]
            scan_ids = [45, 46]
            await pq.set_processed_item_as_completed(
                exit_status=exit_status,
                run_uids=run_uids,
                scan_ids=scan_ids
            )
            
            # Verify item is no longer running
            assert await pq.is_item_running() is False
            
            # Verify history entry
            history, _ = await pq.get_history()
            assert len(history) == 1
            assert history[0]["name"] == "count"
            assert history[0]["result"]["exit_status"] == exit_status
            assert history[0]["result"]["run_uids"] == run_uids
            assert history[0]["result"]["scan_ids"] == scan_ids
    
    asyncio.run(testing())


def test_set_processed_item_as_completed_2():
    """
    Tests completion with loop mode.
    """
    
    async def testing():
        async with PQ() as pq:
            # Set loop mode
            await pq.set_plan_queue_mode({"loop": True, "ignore_failures": False})
            
            # Add and process an item
            item = {"name": "count", "args": [["det1"]]}
            await pq.add_item_to_queue(item)
            original_uid = (await pq.get_queue())[0]["item_uid"]
            await pq.set_next_item_as_running()
            
            # Mark as completed
            await pq.set_processed_item_as_completed(exit_status="completed")
            
            # Verify item is added back to the queue (loop mode)
            assert await pq.get_queue_size() == 1
            queue, _ = await pq.get_queue()
            assert queue[0]["name"] == "count"
            
            # Verify history entry
            history, _ = await pq.get_history()
            assert len(history) == 1
            
            # Verify a new UID was generated for the looped item
            assert queue[0]["item_uid"] != original_uid
            
            # Reset queue mode
            await pq.set_plan_queue_mode({"loop": False, "ignore_failures": False})
    
    asyncio.run(testing())


def test_set_processed_item_as_stopped_1():
    """
    Tests stopping plans with different statuses.
    """
    
    async def testing():
        async with PQ() as pq:
            # Add and process an item
            await pq.add_item_to_queue({"name": "count", "args": [["det1"]]})
            await pq.set_next_item_as_running()
            
            # Mark as failed
            err_msg = "Test error message"
            err_tb = "Test traceback"
            await pq.set_processed_item_as_stopped(
                exit_status="failed",
                err_msg=err_msg,
                err_tb=err_tb
            )
            
            # Verify item is no longer running
            assert await pq.is_item_running() is False
            
            # Verify history entry
            history, _ = await pq.get_history()
            assert len(history) == 1
            assert history[0]["name"] == "count"
            assert history[0]["result"]["exit_status"] == "failed"
            assert history[0]["result"]["err_msg"] == err_msg
            assert history[0]["result"]["err_tb"] == err_tb
            
            # Add and process another item
            await pq.add_item_to_queue({"name": "scan", "args": [["det1"]]})
            await pq.set_next_item_as_running()
            
            # Mark as stopped
            await pq.set_processed_item_as_stopped(exit_status="stopped")
            
            # Verify history has both entries
            history, _ = await pq.get_history()
            assert len(history) == 2
            assert history[1]["name"] == "scan"
            assert history[1]["result"]["exit_status"] == "stopped"
            assert "err_msg" not in history[1]["result"] or not history[1]["result"]["err_msg"]
    
    asyncio.run(testing())


def test_set_processed_item_as_stopped_2():
    """
    Tests multiple stopping scenarios.
    """
    
    async def testing():
        async with PQ() as pq:
            # Set loop mode with ignore_failures=True
            await pq.set_plan_queue_mode({"loop": True, "ignore_failures": True})
            
            # Add and process an item
            item = {"name": "count", "args": [["det1"]]}
            await pq.add_item_to_queue(item)
            original_uid = (await pq.get_queue())[0]["item_uid"]
            await pq.set_next_item_as_running()
            
            # Mark as failed, but should requeue due to ignore_failures=True
            await pq.set_processed_item_as_stopped(exit_status="failed")
            
            # Verify item is added back to the queue
            assert await pq.get_queue_size() == 1
            queue, _ = await pq.get_queue()
            assert queue[0]["name"] == "count"
            
            # Verify a new UID was generated for the looped item
            assert queue[0]["item_uid"] != original_uid
            
            # Process the item again
            await pq.set_next_item_as_running()
            
            # Set loop mode with ignore_failures=False
            await pq.set_plan_queue_mode({"loop": True, "ignore_failures": False})
            
            # Mark as failed, should not requeue due to ignore_failures=False
            await pq.set_processed_item_as_stopped(exit_status="failed")
            
            # Verify queue is empty
            assert await pq.get_queue_size() == 0
            
            # Verify history has both entries
            history, _ = await pq.get_history()
            assert len(history) == 2
            assert history[0]["result"]["exit_status"] == "failed"
            assert history[1]["result"]["exit_status"] == "failed"
            
            # Reset queue mode
            await pq.set_plan_queue_mode({"loop": False, "ignore_failures": False})
    
    asyncio.run(testing())


def test_set_processed_item_as_stopped_3():
    """
    Tests stopping immediate execution plans.
    """
    
    async def testing():
        async with PQ() as pq:
            # Process a direct execution item (not from queue)
            direct_item = {"name": "direct_plan", "args": [["det1"]]}
            await pq.process_next_item(item=direct_item)
            
            # Verify item is running
            assert await pq.is_item_running() is True
            
            # Mark as stopped
            await pq.set_processed_item_as_stopped(exit_status="interrupted")
            
            # Verify item is no longer running
            assert await pq.is_item_running() is False
            
            # Verify history contains the item
            history, _ = await pq.get_history()
            assert len(history) == 1
            assert history[0]["name"] == "direct_plan"
            assert history[0]["result"]["exit_status"] == "interrupted"
    
    asyncio.run(testing())

# 9. History Management

def test_add_to_history_functions():
    """
    Tests adding plans to history.
    """
    
    async def testing():
        async with PQ() as pq:
            # Initially history is empty
            history, _ = await pq.get_history()
            assert len(history) == 0
            assert await pq.get_history_size() == 0
            
            # Add item to history directly
            item1 = {
                "name": "count",
                "args": [["det1"]],
                "kwargs": {"num": 5},
                "item_uid": "test-uid-1",
                "result": {"exit_status": "completed", "run_uids": ["abc123"]}
            }
            
            await pq._add_to_history(item1)
            
            # Verify history
            assert await pq.get_history_size() == 1
            history, history_uid = await pq.get_history()
            assert len(history) == 1
            assert history[0]["name"] == "count"
            assert history[0]["result"]["exit_status"] == "completed"
            assert history[0]["result"]["run_uids"] == ["abc123"]
            assert history[0]["item_uid"] == "test-uid-1"
            
            # Add another item
            item2 = {
                "name": "scan",
                "args": [["det1"], "motor", -1, 1, 10],
                "item_uid": "test-uid-2",
                "result": {"exit_status": "failed", "err_msg": "Test error"}
            }
            
            await pq._add_to_history(item2)
            
            # Verify history
            assert await pq.get_history_size() == 2
            history, _ = await pq.get_history()
            assert len(history) == 2
            assert history[1]["name"] == "scan"
            assert history[1]["result"]["exit_status"] == "failed"
            
            # Test getting history by UID
            item1_from_history = await pq.get_history_item(uid="test-uid-1")
            assert item1_from_history["name"] == "count"
            
            # Test clear history
            await pq.clear_history()
            assert await pq.get_history_size() == 0
            history, _ = await pq.get_history()
            assert len(history) == 0
    
    asyncio.run(testing())

# 10. Configuration Management

def test_set_plan_queue_mode_1():
    """
    Tests queue mode configuration.
    """
    
    async def testing():
        async with PQ() as pq:
            # Check default mode
            default_mode = pq.plan_queue_mode
            assert default_mode["loop"] is False
            assert default_mode["ignore_failures"] is False
            
            # Set new mode
            new_mode = {"loop": True, "ignore_failures": True}
            await pq.set_plan_queue_mode(new_mode, update=False)
            
            # Check mode was changed
            current_mode = pq.plan_queue_mode
            assert current_mode["loop"] is True
            assert current_mode["ignore_failures"] is True
            
            # Update mode partially
            update_mode = {"loop": False}
            await pq.set_plan_queue_mode(update_mode, update=True)
            
            # Check only specified fields were updated
            current_mode = pq.plan_queue_mode
            assert current_mode["loop"] is False  # Updated
            assert current_mode["ignore_failures"] is True  # Unchanged
            
            # Reset to default
            await pq.set_plan_queue_mode("default", update=False)
            
            # Check default was restored
            current_mode = pq.plan_queue_mode
            assert current_mode["loop"] is False
            assert current_mode["ignore_failures"] is False
            
            # Verify persistence across operations
            await pq.set_plan_queue_mode({"loop": True}, update=True)
            
            # Add and process an item
            await pq.add_item_to_queue({"name": "test_plan"})
            await pq.set_next_item_as_running()
            await pq.set_processed_item_as_completed(exit_status="completed")
            
            # Mode should be preserved
            current_mode = pq.plan_queue_mode
            assert current_mode["loop"] is True
    
    asyncio.run(testing())


def test_user_group_permissions_1():
    """
    Tests user permission persistence.
    """
    
    async def testing():
        async with PQ() as pq:
            # Initially permissions are empty
            perms = await pq.user_group_permissions_retrieve()
            assert perms is None
            
            # Set permissions
            permissions = {
                "admin": {"queue_read": True, "queue_write": True},
                "users": {"queue_read": True, "queue_write": False},
                "beamline": {"queue_read": True, "queue_write": True}
            }
            
            await pq.user_group_permissions_save(permissions)
            
            # Verify permissions were saved
            saved_perms = await pq.user_group_permissions_retrieve()
            assert saved_perms == permissions
            
            # Update permissions
            updated_permissions = {
                "admin": {"queue_read": True, "queue_write": True},
                "users": {"queue_read": False, "queue_write": False},  # Changed
                "beamline": {"queue_read": True, "queue_write": True},
                "visitors": {"queue_read": True, "queue_write": False}  # Added
            }
            
            await pq.user_group_permissions_save(updated_permissions)
            
            # Verify updated permissions
            saved_perms = await pq.user_group_permissions_retrieve()
            assert saved_perms == updated_permissions
            assert saved_perms["users"]["queue_read"] is False
            assert "visitors" in saved_perms
            
            # Clear permissions
            await pq.user_group_permissions_clear()
            
            # Verify permissions are cleared
            perms = await pq.user_group_permissions_retrieve()
            assert perms is None
            
            # Test persistence across queue operations
            await pq.user_group_permissions_save(permissions)
            
            # Add and manipulate queue items
            await pq.add_item_to_queue({"name": "test_plan"})
            await pq.add_item_to_queue({"name": "another_plan"})
            await pq.move_item(pos=0, pos_dest=1)
            
            # Permissions should still be present
            saved_perms = await pq.user_group_permissions_retrieve()
            assert saved_perms == permissions
    
    asyncio.run(testing())


def test_lock_info_1():
    """
    Tests lock information persistence.
    """
    
    async def testing():
        async with PQ() as pq:
            # Initially lock info is empty
            lock = await pq.lock_info_retrieve()
            assert lock is None
            
            # Set lock info
            lock_info = {
                "environment": True,
                "queue": False,
                "lock_key": "test_key_123",
                "user": "test_user",
                "time": 1622548800.0
            }
            
            await pq.lock_info_save(lock_info)
            
            # Verify lock info was saved
            saved_lock = await pq.lock_info_retrieve()
            assert saved_lock == lock_info
            
            # Update lock info
            updated_lock = {
                "environment": False,  # Changed
                "queue": True,         # Changed
                "lock_key": "new_key_456",  # Changed
                "user": "test_user",
                "time": 1622548900.0   # Changed
            }
            
            await pq.lock_info_save(updated_lock)
            
            # Verify updated lock info
            saved_lock = await pq.lock_info_retrieve()
            assert saved_lock == updated_lock
            assert saved_lock["environment"] is False
            assert saved_lock["queue"] is True
            assert saved_lock["lock_key"] == "new_key_456"
            
            # Clear lock info
            await pq.lock_info_clear()
            
            # Verify lock info is cleared
            lock = await pq.lock_info_retrieve()
            assert lock is None
            
            # Test persistence across queue operations
            await pq.lock_info_save(lock_info)
            
            # Add and process queue items
            await pq.add_item_to_queue({"name": "test_plan"})
            await pq.set_next_item_as_running()
            await pq.set_processed_item_as_completed(exit_status="completed")
            
            # Lock info should still be present
            saved_lock = await pq.lock_info_retrieve()
            assert saved_lock == lock_info
    
    asyncio.run(testing())


def test_stop_pending_info_1():
    """
    Tests stop pending flag management.
    """
    
    async def testing():
        async with PQ() as pq:
            # Initially stop pending info is empty
            stop_info = await pq.stop_pending_retrieve()
            assert stop_info is None
            
            # Set stop pending info
            stop_pending = {
                "option": "immediate",
                "user": "test_user",
                "time": 1622549000.0
            }
            
            await pq.stop_pending_save(stop_pending)
            
            # Verify stop pending info was saved
            saved_stop = await pq.stop_pending_retrieve()
            assert saved_stop == stop_pending
            
            # Update stop pending info
            updated_stop = {
                "option": "deferred",  # Changed
                "user": "another_user", # Changed
                "time": 1622549100.0    # Changed
            }
            
            await pq.stop_pending_save(updated_stop)
            
            # Verify updated stop pending info
            saved_stop = await pq.stop_pending_retrieve()
            assert saved_stop == updated_stop
            assert saved_stop["option"] == "deferred"
            assert saved_stop["user"] == "another_user"
            
            # Clear stop pending info
            await pq.stop_pending_clear()
            
            # Verify stop pending info is cleared
            stop_info = await pq.stop_pending_retrieve()
            assert stop_info is None
            
            # Test persistence across queue operations
            await pq.stop_pending_save(stop_pending)
            
            # Add items to queue
            await pq.add_item_to_queue({"name": "test_plan"})
            await pq.add_item_to_queue({"name": "another_plan"})
            
            # Stop pending info should still be present
            saved_stop = await pq.stop_pending_retrieve()
            assert saved_stop == stop_pending
    
    asyncio.run(testing())


def test_autostart_mode_info_1():
    """
    Tests autostart mode configuration.
    """
    
    async def testing():
        async with PQ() as pq:
            # Initially autostart mode info is empty
            auto_info = await pq.autostart_mode_retrieve()
            assert auto_info is None
            
            # Set autostart mode info
            autostart_mode = {
                "enabled": True,
                "restart_pending_plans": False,
                "user": "test_user",
                "time": 1622549200.0
            }
            
            await pq.autostart_mode_save(autostart_mode)
            
            # Verify autostart mode info was saved
            saved_auto = await pq.autostart_mode_retrieve()
            assert saved_auto == autostart_mode
            
            # Update autostart mode info
            updated_auto = {
                "enabled": False,           # Changed
                "restart_pending_plans": True,  # Changed
                "user": "another_user",     # Changed
                "time": 1622549300.0        # Changed
            }
            
            await pq.autostart_mode_save(updated_auto)
            
            # Verify updated autostart mode info
            saved_auto = await pq.autostart_mode_retrieve()
            assert saved_auto == updated_auto
            assert saved_auto["enabled"] is False
            assert saved_auto["restart_pending_plans"] is True
            
            # Clear autostart mode info
            await pq.autostart_mode_clear()
            
            # Verify autostart mode info is cleared
            auto_info = await pq.autostart_mode_retrieve()
            assert auto_info is None
            
            # Test cross-instance persistence with file-based database
            # Create a new test with temporary file
            db_path = "temp_autostart_test.db"
            try:
                # First session
                async with PQ(db_path=db_path) as pq1:
                    await pq1.autostart_mode_save(autostart_mode)
                
                # Second session with same DB file
                async with PQ(db_path=db_path) as pq2:
                    # Verify data persisted
                    saved_auto = await pq2.autostart_mode_retrieve()
                    assert saved_auto == autostart_mode
            finally:
                # Clean up file
                if os.path.exists(db_path):
                    try:
                        os.unlink(db_path)
                    except:
                        pass
    
    asyncio.run(testing())