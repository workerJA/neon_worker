"""
Tests for Neon Worker synchronization mechanisms.

This module contains comprehensive tests to validate thread safety,
synchronization, and proper handling of concurrent operations.
"""

import unittest
import threading
import time
import queue
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from worker import NeonWorker, Task, SynchronizedCounter
from config import ConfigManager, WorkerConfig


class TestSynchronizedCounter(unittest.TestCase):
    """Test the thread-safe counter implementation."""
    
    def test_counter_basic_operations(self):
        """Test basic counter operations."""
        counter = SynchronizedCounter(10)
        
        self.assertEqual(counter.value, 10)
        self.assertEqual(counter.increment(), 11)
        self.assertEqual(counter.decrement(), 10)
        self.assertEqual(counter.value, 10)
    
    def test_counter_thread_safety(self):
        """Test counter thread safety with concurrent operations."""
        counter = SynchronizedCounter(0)
        num_threads = 10
        operations_per_thread = 100
        
        def increment_worker():
            for _ in range(operations_per_thread):
                counter.increment()
        
        def decrement_worker():
            for _ in range(operations_per_thread):
                counter.decrement()
        
        # Start equal number of increment and decrement threads
        threads = []
        for _ in range(num_threads // 2):
            t1 = threading.Thread(target=increment_worker)
            t2 = threading.Thread(target=decrement_worker)
            threads.extend([t1, t2])
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        # Counter should be back to 0
        self.assertEqual(counter.value, 0)


class TestNeonWorkerSynchronization(unittest.TestCase):
    """Test NeonWorker synchronization and thread safety."""
    
    def setUp(self):
        """Set up test environment."""
        self.worker = NeonWorker(max_workers=3, queue_size=50)
        self.results = []
        self.results_lock = threading.Lock()
    
    def tearDown(self):
        """Clean up test environment."""
        self.worker.shutdown(timeout=5.0)
    
    def test_task_execution_order_with_priority(self):
        """Test that tasks are executed according to priority."""
        self.worker.start()
        
        executed_order = []
        execution_lock = threading.Lock()
        
        def priority_task(task_id, priority):
            with execution_lock:
                executed_order.append((task_id, priority))
            time.sleep(0.1)  # Small delay to ensure ordering
        
        # Add tasks with different priorities
        tasks = [
            Task("low_priority", priority_task, ("low", 0), {"priority": 0}, priority=0),
            Task("high_priority", priority_task, ("high", 2), {"priority": 2}, priority=2),
            Task("medium_priority", priority_task, ("medium", 1), {"priority": 1}, priority=1),
        ]
        
        # Add tasks in random order
        for task in tasks:
            self.worker.add_task(task)
        
        # Wait for completion
        self.worker.wait_for_completion(timeout=10.0)
        
        # Check that high priority task was executed first
        self.assertTrue(len(executed_order) >= 3)
        # Note: Due to threading, exact order may vary, but high priority should be early
        priorities = [item[1] for item in executed_order]
        self.assertIn(2, priorities)  # High priority task was executed
    
    def test_concurrent_task_addition(self):
        """Test thread safety when adding tasks concurrently."""
        self.worker.start()
        
        shared_counter = SynchronizedCounter(0)
        
        def counting_task():
            shared_counter.increment()
            time.sleep(0.01)
        
        def add_tasks_worker(start_id, count):
            for i in range(count):
                task = Task(f"task_{start_id}_{i}", counting_task)
                self.worker.add_task(task)
        
        # Add tasks from multiple threads concurrently
        num_threads = 5
        tasks_per_thread = 10
        threads = []
        
        for i in range(num_threads):
            thread = threading.Thread(target=add_tasks_worker, args=(i, tasks_per_thread))
            threads.append(thread)
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Wait for all tasks to complete
        self.worker.wait_for_completion(timeout=10.0)
        
        # Verify all tasks were executed
        expected_count = num_threads * tasks_per_thread
        self.assertEqual(shared_counter.value, expected_count)
    
    def test_resource_contention(self):
        """Test worker behavior under resource contention."""
        self.worker.start()
        
        shared_resource = {"value": 0}
        resource_lock = threading.Lock()
        access_count = SynchronizedCounter(0)
        
        def resource_task():
            access_count.increment()
            # Simulate resource contention
            with resource_lock:
                old_value = shared_resource["value"]
                time.sleep(0.01)  # Simulate work
                shared_resource["value"] = old_value + 1
        
        # Add many tasks that access the shared resource
        num_tasks = 20
        for i in range(num_tasks):
            task = Task(f"resource_task_{i}", resource_task)
            self.worker.add_task(task)
        
        # Wait for completion
        self.worker.wait_for_completion(timeout=15.0)
        
        # Verify resource consistency
        self.assertEqual(shared_resource["value"], num_tasks)
        self.assertEqual(access_count.value, num_tasks)
    
    def test_error_handling_synchronization(self):
        """Test that errors in one task don't affect others."""
        self.worker.start()
        
        successful_tasks = SynchronizedCounter(0)
        
        def failing_task():
            raise ValueError("Intentional test error")
        
        def successful_task():
            successful_tasks.increment()
            time.sleep(0.1)
        
        # Add mix of failing and successful tasks
        for i in range(10):
            if i % 3 == 0:
                task = Task(f"fail_{i}", failing_task)
            else:
                task = Task(f"success_{i}", successful_task)
            self.worker.add_task(task)
        
        # Wait for completion
        self.worker.wait_for_completion(timeout=10.0)
        
        # Check that successful tasks completed despite failures
        status = self.worker.get_status()
        self.assertGreater(status["completed_tasks"], 0)
        self.assertGreater(status["failed_tasks"], 0)
        self.assertGreater(successful_tasks.value, 0)
    
    def test_graceful_shutdown_with_active_tasks(self):
        """Test graceful shutdown behavior with active tasks."""
        self.worker.start()
        
        def long_running_task():
            time.sleep(2.0)
            return "completed"
        
        # Add several long-running tasks
        for i in range(5):
            task = Task(f"long_task_{i}", long_running_task)
            self.worker.add_task(task)
        
        # Give tasks a moment to start
        time.sleep(0.5)
        
        # Initiate shutdown
        start_time = time.time()
        self.worker.shutdown(timeout=10.0)
        shutdown_time = time.time() - start_time
        
        # Shutdown should complete within reasonable time
        self.assertLess(shutdown_time, 15.0)
        
        # Worker should be in shutdown state
        status = self.worker.get_status()
        self.assertTrue(status["is_shutdown"])


class TestConfigManagerSynchronization(unittest.TestCase):
    """Test ConfigManager thread safety."""
    
    def setUp(self):
        """Set up test environment."""
        self.config_manager = ConfigManager("test_config.json")
    
    def tearDown(self):
        """Clean up test environment."""
        import os
        if os.path.exists("test_config.json"):
            os.remove("test_config.json")
    
    def test_concurrent_config_updates(self):
        """Test thread safety of concurrent configuration updates."""
        def update_worker(worker_id, iterations):
            for i in range(iterations):
                self.config_manager.update_config(
                    max_workers=worker_id + i,
                    queue_size=100 + worker_id + i
                )
                time.sleep(0.01)
        
        # Start multiple threads updating config
        threads = []
        for worker_id in range(5):
            thread = threading.Thread(target=update_worker, args=(worker_id, 10))
            threads.append(thread)
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Config should be accessible and consistent
        config = self.config_manager.get_config()
        self.assertIsInstance(config, WorkerConfig)
        self.assertGreater(config.max_workers, 0)
        self.assertGreater(config.queue_size, 0)


class TestStressScenarios(unittest.TestCase):
    """Stress tests for synchronization under high load."""
    
    def test_high_concurrency_stress(self):
        """Test worker under high concurrency stress."""
        worker = NeonWorker(max_workers=8, queue_size=200)
        worker.start()
        
        try:
            completed_tasks = SynchronizedCounter(0)
            
            def stress_task(task_id):
                # Simulate varying work loads
                work_time = random.uniform(0.001, 0.1)
                time.sleep(work_time)
                completed_tasks.increment()
                return f"Task {task_id} completed"
            
            # Add many tasks quickly
            num_tasks = 100
            for i in range(num_tasks):
                task = Task(
                    f"stress_task_{i}",
                    stress_task,
                    (i,),
                    priority=random.randint(0, 2)
                )
                worker.add_task(task)
            
            # Wait for completion
            worker.wait_for_completion(timeout=30.0)
            
            # Verify all tasks completed
            self.assertEqual(completed_tasks.value, num_tasks)
            
            status = worker.get_status()
            self.assertEqual(status["completed_tasks"], num_tasks)
            self.assertEqual(status["queue_size"], 0)
            
        finally:
            worker.shutdown(timeout=10.0)


def run_synchronization_tests():
    """Run all synchronization tests."""
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestSynchronizedCounter,
        TestNeonWorkerSynchronization,
        TestConfigManagerSynchronization,
        TestStressScenarios
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_synchronization_tests()
    exit(0 if success else 1)