"""
Simple test to validate basic worker functionality.
"""

import time
from worker import NeonWorker, Task, SynchronizedCounter

def simple_test():
    """Test basic worker functionality."""
    print("Testing basic worker functionality...")
    
    # Test 1: Basic counter
    print("\n1. Testing SynchronizedCounter...")
    counter = SynchronizedCounter(0)
    assert counter.increment() == 1
    assert counter.increment() == 2
    assert counter.decrement() == 1
    assert counter.value == 1
    print("✓ SynchronizedCounter works correctly")
    
    # Test 2: Basic worker operations
    print("\n2. Testing basic worker operations...")
    worker = NeonWorker(max_workers=2, queue_size=10)
    
    results = []
    
    def test_task(task_id):
        results.append(f"Task {task_id} executed")
        time.sleep(0.1)
        return f"Result {task_id}"
    
    worker.start()
    
    # Add some tasks
    for i in range(5):
        task = Task(f"task_{i}", test_task, (i,))
        success = worker.add_task(task)
        assert success, f"Failed to add task {i}"
    
    # Wait for completion
    worker.wait_for_completion(timeout=10.0)
    
    # Check status
    status = worker.get_status()
    print(f"Status: {status}")
    
    # Shutdown
    worker.shutdown(timeout=5.0)
    
    assert len(results) == 5, f"Expected 5 results, got {len(results)}"
    print("✓ Basic worker operations work correctly")
    
    print("\n3. Testing error handling...")
    worker2 = NeonWorker(max_workers=1, queue_size=5)
    worker2.start()
    
    def failing_task():
        raise ValueError("Test error")
    
    def working_task():
        return "success"
    
    # Add failing and working tasks
    worker2.add_task(Task("fail", failing_task))
    worker2.add_task(Task("work", working_task))
    
    worker2.wait_for_completion(timeout=5.0)
    status = worker2.get_status()
    
    worker2.shutdown(timeout=5.0)
    
    assert status["failed_tasks"] >= 1, "Should have failed tasks"
    assert status["completed_tasks"] >= 1, "Should have completed tasks"
    print("✓ Error handling works correctly")
    
    print("\n✅ All basic tests passed!")

if __name__ == "__main__":
    simple_test()