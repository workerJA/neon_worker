#!/usr/bin/env python3
"""
Neon Worker - Synchronization Demonstration

This script demonstrates the synchronization solution in action,
showing how the worker handles various concurrent scenarios safely.
"""

import time
import threading
import random
from worker import NeonWorker, Task, SynchronizedCounter
from config import get_config, update_config

def demonstrate_synchronization():
    """Demonstrate various synchronization scenarios."""
    
    print("🚀 Neon Worker - Synchronization Demonstration")
    print("=" * 50)
    
    # Demo 1: Basic Thread-Safe Operations
    print("\n📊 Demo 1: Thread-Safe Counter Operations")
    counter = SynchronizedCounter(0)
    
    def counter_worker(worker_id, operations):
        for i in range(operations):
            if i % 2 == 0:
                value = counter.increment()
                print(f"Worker {worker_id}: Incremented to {value}")
            else:
                value = counter.decrement()
                print(f"Worker {worker_id}: Decremented to {value}")
            time.sleep(0.1)
    
    # Start multiple threads manipulating the counter
    threads = []
    for i in range(3):
        thread = threading.Thread(target=counter_worker, args=(i, 5))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    print(f"✅ Final counter value: {counter.value}")
    
    # Demo 2: Priority Task Execution
    print("\n🎯 Demo 2: Priority-Based Task Execution")
    worker = NeonWorker(max_workers=2, queue_size=20)
    worker.start()
    
    execution_order = []
    order_lock = threading.Lock()
    
    def priority_task(task_name, priority, delay=0.2):
        time.sleep(delay)
        with order_lock:
            execution_order.append((task_name, priority))
            print(f"Executed: {task_name} (priority {priority})")
    
    # Add tasks with different priorities
    tasks = [
        ("Low Priority Task 1", 0),
        ("High Priority Task", 2),
        ("Medium Priority Task", 1),
        ("Low Priority Task 2", 0),
        ("Critical Task", 3),
    ]
    
    print("Adding tasks in random order...")
    for task_name, priority in tasks:
        task = Task(
            id=task_name.replace(" ", "_"),
            function=priority_task,
            args=(task_name, priority),
            priority=priority
        )
        worker.add_task(task)
        print(f"Added: {task_name} (priority {priority})")
    
    worker.wait_for_completion(timeout=10.0)
    
    print("\nExecution order:")
    for task_name, priority in execution_order:
        print(f"  {task_name} (priority {priority})")
    
    worker.shutdown(timeout=5.0)
    
    # Demo 3: Concurrent Data Processing
    print("\n⚡ Demo 3: Concurrent Data Processing with Resource Contention")
    
    # Shared resource that needs synchronization
    shared_database = {"records": [], "lock": threading.Lock()}
    processing_stats = SynchronizedCounter(0)
    
    def data_processor(batch_id, data_items):
        """Simulate processing data with shared resource access."""
        results = []
        
        for item in data_items:
            # Simulate processing time
            time.sleep(random.uniform(0.05, 0.15))
            
            processed_item = f"processed_{batch_id}_{item}"
            results.append(processed_item)
            
            # Thread-safe database update
            with shared_database["lock"]:
                shared_database["records"].append(processed_item)
            
            processing_stats.increment()
        
        print(f"Batch {batch_id}: Processed {len(data_items)} items")
        return results
    
    worker2 = NeonWorker(max_workers=4, queue_size=30)
    worker2.start()
    
    # Create batches of data to process
    print("Creating data processing tasks...")
    for batch_id in range(5):
        data_items = [f"item_{i}" for i in range(batch_id * 3, (batch_id + 1) * 3)]
        
        task = Task(
            id=f"batch_{batch_id}",
            function=data_processor,
            args=(batch_id, data_items),
            priority=1
        )
        worker2.add_task(task)
    
    print("Processing data concurrently...")
    worker2.wait_for_completion(timeout=15.0)
    
    print(f"✅ Processed {processing_stats.value} items total")
    print(f"✅ Database contains {len(shared_database['records'])} records")
    
    worker2.shutdown(timeout=5.0)
    
    # Demo 4: Error Isolation
    print("\n🛡️  Demo 4: Error Isolation and Recovery")
    
    successful_tasks = SynchronizedCounter(0)
    failed_tasks = SynchronizedCounter(0)
    
    def unreliable_task(task_id, failure_rate=0.3):
        """Task that randomly fails to test error handling."""
        if random.random() < failure_rate:
            failed_tasks.increment()
            raise RuntimeError(f"Task {task_id} failed randomly")
        
        time.sleep(0.1)
        successful_tasks.increment()
        return f"Task {task_id} completed successfully"
    
    worker3 = NeonWorker(max_workers=3, queue_size=25)
    worker3.start()
    
    print("Adding mix of reliable and unreliable tasks...")
    for i in range(15):
        task = Task(
            id=f"unreliable_task_{i}",
            function=unreliable_task,
            args=(i,),
            kwargs={"failure_rate": 0.4}
        )
        worker3.add_task(task)
    
    worker3.wait_for_completion(timeout=10.0)
    
    status = worker3.get_status()
    print(f"✅ Successful tasks: {successful_tasks.value}")
    print(f"❌ Failed tasks: {failed_tasks.value}")
    print(f"📊 Worker status: {status['completed_tasks']} completed, {status['failed_tasks']} failed")
    
    worker3.shutdown(timeout=5.0)
    
    # Demo 5: Configuration Management
    print("\n⚙️  Demo 5: Thread-Safe Configuration Management")
    
    print("Current configuration:")
    config = get_config()
    print(f"  Max workers: {config.max_workers}")
    print(f"  Queue size: {config.queue_size}")
    print(f"  Retry attempts: {config.retry_attempts}")
    
    print("\nUpdating configuration from multiple threads...")
    
    def config_updater(worker_id):
        for i in range(3):
            update_config(
                max_workers=worker_id + 2,
                queue_size=50 + worker_id * 10
            )
            print(f"Worker {worker_id}: Updated configuration")
            time.sleep(0.1)
    
    config_threads = []
    for i in range(3):
        thread = threading.Thread(target=config_updater, args=(i,))
        config_threads.append(thread)
        thread.start()
    
    for thread in config_threads:
        thread.join()
    
    final_config = get_config()
    print(f"✅ Final configuration - Max workers: {final_config.max_workers}, Queue size: {final_config.queue_size}")
    
    print("\n" + "=" * 50)
    print("🎉 All synchronization demonstrations completed successfully!")
    print("   The worker handled all concurrent scenarios safely.")

if __name__ == "__main__":
    demonstrate_synchronization()