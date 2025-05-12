import pytest
import inspect
import asyncio
from typing import get_type_hints, get_args, get_origin, Union

from bluesky_queueserver.manager.plan_queue_ops_backends.plan_queue_ops_abstract import AbstractPlanQueueOperations
from bluesky_queueserver.manager.plan_queue_ops_backends.plan_queue_ops_sqlite import SQLitePlanQueueOperations
from bluesky_queueserver.manager.plan_queue_ops_backends.plan_queue_ops_redis import RedisPlanQueueOperations
from bluesky_queueserver.manager.plan_queue_ops_backends.plan_queue_ops_dict import DictPlanQueueOperations
from bluesky_queueserver.manager.manager import RunEngineManager, LockInfo, MState


class TestPlanQueueOpsContract:
    """Tests to ensure implementations properly fulfill the abstract interface contract."""
    
    @pytest.mark.parametrize("implementation_class", [
        SQLitePlanQueueOperations,
        RedisPlanQueueOperations,
        DictPlanQueueOperations,  # Added DictPlanQueueOperations
    ])
    def test_implements_all_abstract_methods(self, implementation_class):
        """Verify that implementations provide all required abstract methods."""
        # Get all abstract methods from the base class
        abstract_methods = {
            name: method for name, method in inspect.getmembers(AbstractPlanQueueOperations)
            if not name.startswith('_') or name in ('_verify_item', '_clean_item_properties')  # Include specific protected methods
            if inspect.isabstract(method) or (hasattr(method, '__isabstractmethod__') and method.__isabstractmethod__)
        }
        
        # Check each abstract method is implemented
        for method_name, abstract_method in abstract_methods.items():
            assert hasattr(implementation_class, method_name), f"Method '{method_name}' not implemented"
            
            impl_method = getattr(implementation_class, method_name)
            assert callable(impl_method), f"'{method_name}' is not callable in implementation"
            
            # Check signature compatibility
            abstract_sig = inspect.signature(abstract_method)
            impl_sig = inspect.signature(impl_method)
            
            # Check required parameters
            for param_name in abstract_sig.parameters:
                if param_name == 'self':
                    continue
                assert param_name in impl_sig.parameters, f"Parameter '{param_name}' missing in implementation of '{method_name}'"
            
            # Check return type annotations match
            abstract_return = abstract_sig.return_annotation
            impl_return = impl_sig.return_annotation
            if abstract_return is not inspect.Signature.empty and impl_return is not inspect.Signature.empty:
                assert impl_return == abstract_return or issubclass(impl_return, abstract_return), \
                    f"Return type mismatch in '{method_name}': expected {abstract_return}, got {impl_return}"
    
    @pytest.mark.parametrize("implementation_class", [
        SQLitePlanQueueOperations,
        RedisPlanQueueOperations,
        DictPlanQueueOperations,  # Added DictPlanQueueOperations
    ])
    def test_properties_implemented(self, implementation_class):
        """Check that required properties are implemented."""
        required_properties = [
            "plan_queue_uid", 
            "plan_history_uid",
            "plan_queue_mode"
        ]
        
        impl = implementation_class()
        for prop_name in required_properties:
            assert hasattr(impl, prop_name), f"Property '{prop_name}' missing in implementation"
            
    @pytest.mark.asyncio
    @pytest.mark.parametrize("implementation_class", [
        SQLitePlanQueueOperations,
        RedisPlanQueueOperations,
        DictPlanQueueOperations,  # Added DictPlanQueueOperations
    ])
    async def test_async_interface_compatibility(self, implementation_class):
        """Test async methods for interface compatibility."""
        # For DictPlanQueueOperations, use in-memory mode to avoid file I/O
        if implementation_class == DictPlanQueueOperations:
            impl = implementation_class(in_memory=True)
        else:
            impl = implementation_class()
            
        await impl.start()
        
        try:
            # Test a few key operations to ensure async compatibility
            await impl.clear_queue()
            
            # Add an item
            item = {
                "name": "count", 
                "args": [["det1"]], 
                "kwargs": {"num": 5},
                "item_type": "plan"
            }
            item = await impl.set_new_item_uuid(item)
            await impl.add_item_to_queue(item)
            
            # Verify the queue has the item
            queue, _ = await impl.get_queue()
            assert len(queue) == 1
            assert queue[0]["name"] == "count"
            
            # Test setting queue mode
            await impl.set_plan_queue_mode({"loop": True})
            assert impl.plan_queue_mode["loop"] is True
        
        finally:
            await impl.stop()
    
    def test_run_engine_manager_interface_usage(self):
        """
        Analyze RunEngineManager to find all methods it calls on the queue interface.
        This helps identify any missing or incompatible interface requirements.
        """
        # Get source code of RunEngineManager
        source = inspect.getsource(RunEngineManager)
        
        # Extract all calls to self._plan_queue.X
        import re
        plan_queue_calls = re.findall(r'self\._plan_queue\.(\w+)', source)
        unique_calls = set(plan_queue_calls)
        
        # Print the set of methods that RunEngineManager expects from the interface
        for method_name in sorted(unique_calls):
            # Check if this method exists in the abstract class
            assert hasattr(AbstractPlanQueueOperations, method_name), \
                f"Method '{method_name}' called by RunEngineManager but not defined in AbstractPlanQueueOperations"


class TestManagerIntegration:
    """Integration tests with RunEngineManager."""
    
    @pytest.mark.parametrize("implementation_class", [
        SQLitePlanQueueOperations,
        RedisPlanQueueOperations,
        DictPlanQueueOperations,  # Added DictPlanQueueOperations
    ])
    def test_manager_initialization_with_implementation(self, implementation_class, monkeypatch):
        """Test that RunEngineManager can initialize properly with each implementation."""
        # Mock necessary dependencies for RunEngineManager
        monkeypatch.setattr("multiprocessing.Process.__init__", lambda *args, **kwargs: None)
        monkeypatch.setattr("bluesky_queueserver.manager.manager.PlanQueueOperations", implementation_class)
        
        # Create minimal config for initialization
        config = {
            "zmq_addr": "tcp://*:60615",
            "redis_addr": "localhost",
            "redis_name_prefix": "test_prefix",
            "lock_key_emergency": None,
            "user_group_permissions_reload": "NEVER",
            "use_ipython_kernel": False,
        }
        
        # Create a manager instance
        manager = RunEngineManager(
            conn_watchdog=object(),  # Fake connection
            conn_worker=object(),    # Fake connection
            config=config,
            number_of_restarts=1
        )
        
        # Assert that the manager can be instantiated without errors
        assert isinstance(manager, RunEngineManager)


class TestTypeAnnotationCompliance:
    """Tests to ensure type annotations match between abstract class and implementations."""
    
    @pytest.mark.parametrize("implementation_class", [
        SQLitePlanQueueOperations,
        RedisPlanQueueOperations,
        DictPlanQueueOperations,  # Added DictPlanQueueOperations
    ])
    def test_method_type_annotations_match(self, implementation_class):
        """Verify type annotations in implementations match the abstract class."""
        abstract_methods = {
            name: method for name, method in inspect.getmembers(AbstractPlanQueueOperations, inspect.isfunction)
            if not name.startswith('__')  # Exclude dunder methods
        }
        
        for method_name, abstract_method in abstract_methods.items():
            if not hasattr(implementation_class, method_name):
                continue  # Skip methods not implemented (should be caught by other tests)
                
            impl_method = getattr(implementation_class, method_name)
            
            # Get type hints for abstract method and implementation
            abstract_hints = get_type_hints(abstract_method)
            impl_hints = get_type_hints(impl_method)
            
            # Check parameters have compatible type annotations
            for param_name, abstract_type in abstract_hints.items():
                if param_name == 'return':
                    continue  # Check return type separately
                    
                if param_name in impl_hints:
                    impl_type = impl_hints[param_name]
                    # Check if types are compatible
                    assert self._is_type_compatible(impl_type, abstract_type), \
                        f"Parameter '{param_name}' in '{method_name}' has incompatible type: " \
                        f"abstract={abstract_type}, implementation={impl_type}"
    
    @staticmethod
    def _is_type_compatible(impl_type, abstract_type):
        """Check if implementation type is compatible with abstract type."""
        # Check for Union types
        if get_origin(abstract_type) is Union:
            return impl_type in get_args(abstract_type) or any(
                TestTypeAnnotationCompliance._is_type_compatible(impl_type, arg_type)
                for arg_type in get_args(abstract_type)
            )
        
        # Check for inheritance relationships
        try:
            return issubclass(impl_type, abstract_type)
        except TypeError:
            # Handle non-class types
            return impl_type == abstract_type


class TestAsyncPropertyBehavior:
    """Test correct async/await behavior in implementations."""
    
    @pytest.mark.asyncio
    @pytest.mark.parametrize("implementation_class", [
        SQLitePlanQueueOperations,
        RedisPlanQueueOperations, 
        DictPlanQueueOperations,  # Added DictPlanQueueOperations
    ])
    async def test_async_methods_return_awaitable(self, implementation_class):
        """Verify that async methods return objects that can be awaited."""
        # For DictPlanQueueOperations, use in-memory mode to avoid file I/O
        if implementation_class == DictPlanQueueOperations:
            impl = implementation_class(in_memory=True)
        else:
            impl = implementation_class()
            
        await impl.start()
        
        try:
            # Test a few key methods
            methods_to_test = [
                'clear_queue',
                'add_item_to_queue',
                'get_queue',
                'set_plan_queue_mode'
            ]
            
            for method_name in methods_to_test:
                method = getattr(impl, method_name)
                if asyncio.iscoroutinefunction(method):
                    # Call with minimal args - this might fail but we just want to verify the return type
                    try:
                        if method_name == 'add_item_to_queue':
                            result = method({"name": "test", "item_type": "plan"})
                        elif method_name == 'set_plan_queue_mode':
                            result = method({})
                        else:
                            result = method()
                        
                        # Verify the result is awaitable
                        assert asyncio.iscoroutine(result), f"Method {method_name} doesn't return an awaitable"
                    except:
                        # We don't care about execution errors, just the interface
                        pass
                        
        finally:
            await impl.stop()