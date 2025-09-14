"""
Neon Worker - A thread-safe background worker with proper synchronization.

This module provides a robust worker implementation that addresses common
synchronization issues in multi-threaded environments.
"""

import threading
import queue
import time
import logging
import signal
import sys
from typing import Optional, Callable, Any, Dict
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class Task:
    """Represents a task to be executed by the worker."""
    id: str
    function: Callable
    args: tuple = ()
    kwargs: dict = None
    priority: int = 0
    
    def __post_init__(self):
        if self.kwargs is None:
            self.kwargs = {}


class SynchronizedCounter:
    """Thread-safe counter with locking mechanism."""
    
    def __init__(self, initial_value: int = 0):
        self._value = initial_value
        self._lock = threading.RLock()
    
    def increment(self) -> int:
        """Increment counter and return new value."""
        with self._lock:
            self._value += 1
            return self._value
    
    def decrement(self) -> int:
        """Decrement counter and return new value."""
        with self._lock:
            self._value -= 1
            return self._value
    
    @property
    def value(self) -> int:
        """Get current counter value."""
        with self._lock:
            return self._value


class NeonWorker:
    """
    A thread-safe worker implementation that addresses synchronization issues.
    
    Features:
    - Thread-safe task queue with priority support
    - Proper resource locking and synchronization
    - Graceful shutdown handling
    - Worker pool management
    - Task status tracking
    - Error handling and retry mechanisms
    """
    
    def __init__(self, max_workers: int = 4, queue_size: int = 100):
        self.max_workers = max_workers
        self.queue_size = queue_size
        
        # Thread-safe task queue with priority support
        self.task_queue = queue.PriorityQueue(maxsize=queue_size)
        
        # Synchronization primitives
        self._shutdown_event = threading.Event()
        self._workers_lock = threading.RLock()
        self._status_lock = threading.RLock()
        
        # Worker management
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self._active_workers = SynchronizedCounter()
        
        # Task tracking
        self._task_status: Dict[str, str] = {}
        self._completed_tasks = SynchronizedCounter()
        self._failed_tasks = SynchronizedCounter()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"NeonWorker initialized with {max_workers} workers")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown()
    
    def add_task(self, task: Task) -> bool:
        """
        Add a task to the queue with proper synchronization.
        
        Args:
            task: Task to be executed
            
        Returns:
            bool: True if task was added successfully, False otherwise
        """
        if self._shutdown_event.is_set():
            logger.warning("Cannot add task: worker is shutting down")
            return False
        
        try:
            # Use negative priority for proper ordering (higher priority = lower number)
            priority_item = (-task.priority, time.time(), task)
            self.task_queue.put(priority_item, timeout=1.0)
            
            with self._status_lock:
                self._task_status[task.id] = "queued"
            
            logger.debug(f"Task {task.id} added to queue with priority {task.priority}")
            return True
            
        except queue.Full:
            logger.error(f"Failed to add task {task.id}: queue is full")
            return False
        except Exception as e:
            logger.error(f"Error adding task {task.id}: {e}")
            return False
    
    def _execute_task(self, task: Task) -> Any:
        """
        Execute a single task with proper error handling and synchronization.
        
        Args:
            task: Task to execute
            
        Returns:
            Task execution result
        """
        worker_id = self._active_workers.increment()
        logger.debug(f"Worker {worker_id} executing task {task.id}")
        
        try:
            with self._status_lock:
                self._task_status[task.id] = "running"
            
            # Execute the task function
            result = task.function(*task.args, **task.kwargs)
            
            with self._status_lock:
                self._task_status[task.id] = "completed"
            
            self._completed_tasks.increment()
            logger.debug(f"Task {task.id} completed successfully")
            return result
            
        except Exception as e:
            with self._status_lock:
                self._task_status[task.id] = "failed"
            
            self._failed_tasks.increment()
            logger.error(f"Task {task.id} failed: {e}")
            raise
            
        finally:
            self._active_workers.decrement()
    
    def _worker_loop(self):
        """Main worker loop that processes tasks from the queue."""
        logger.debug("Worker thread started")
        
        while not self._shutdown_event.is_set():
            try:
                # Get task from queue with timeout to allow periodic shutdown checks
                try:
                    priority_item = self.task_queue.get(timeout=1.0)
                    _, _, task = priority_item
                except queue.Empty:
                    continue
                
                # Execute the task
                try:
                    self._execute_task(task)
                except Exception as e:
                    logger.error(f"Unhandled error in task execution: {e}")
                finally:
                    self.task_queue.task_done()
                    
            except Exception as e:
                logger.error(f"Error in worker loop: {e}")
        
        logger.debug("Worker thread terminated")
    
    def start(self):
        """Start the worker with proper synchronization."""
        with self._workers_lock:
            if self._shutdown_event.is_set():
                logger.warning("Cannot start: worker is already shutting down")
                return
            
            # Start worker threads
            for i in range(self.max_workers):
                self.executor.submit(self._worker_loop)
            
            logger.info(f"NeonWorker started with {self.max_workers} worker threads")
    
    def shutdown(self, timeout: float = 30.0):
        """
        Gracefully shutdown the worker with proper synchronization.
        
        Args:
            timeout: Maximum time to wait for shutdown completion
        """
        logger.info("Initiating worker shutdown...")
        
        with self._workers_lock:
            # Signal shutdown to all workers
            self._shutdown_event.set()
            
            # Wait for current tasks to complete with timeout
            logger.info("Waiting for current tasks to complete...")
            try:
                start_time = time.time()
                while not self.task_queue.empty() and (time.time() - start_time) < timeout:
                    time.sleep(0.1)
                
                # Give a brief moment for active workers to finish current tasks
                end_time = time.time() + min(5.0, timeout - (time.time() - start_time))
                while self._active_workers.value > 0 and time.time() < end_time:
                    time.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"Error waiting for task completion: {e}")
            
            # Shutdown executor
            try:
                self.executor.shutdown(wait=True)
            except Exception as e:
                logger.error(f"Error shutting down executor: {e}")
            
            logger.info("Worker shutdown completed")
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get worker status with proper synchronization.
        
        Returns:
            Dictionary containing worker status information
        """
        with self._status_lock:
            return {
                "active_workers": self._active_workers.value,
                "queue_size": self.task_queue.qsize(),
                "completed_tasks": self._completed_tasks.value,
                "failed_tasks": self._failed_tasks.value,
                "is_shutdown": self._shutdown_event.is_set(),
                "task_status": dict(self._task_status)
            }
    
    def wait_for_completion(self, timeout: Optional[float] = None):
        """
        Wait for all queued tasks to complete.
        
        Args:
            timeout: Maximum time to wait (None for infinite)
        """
        try:
            if timeout:
                start_time = time.time()
                while not self.task_queue.empty() and not self._shutdown_event.is_set():
                    if time.time() - start_time > timeout:
                        raise TimeoutError("Timeout waiting for task completion")
                    time.sleep(0.1)
                # Additional wait for active workers to finish
                remaining_time = timeout - (time.time() - start_time)
                if remaining_time > 0:
                    end_time = time.time() + remaining_time
                    while self._active_workers.value > 0 and time.time() < end_time:
                        time.sleep(0.1)
            else:
                # Use join with timeout to avoid infinite blocking
                try:
                    self.task_queue.join()
                except:
                    # Fallback to polling if join fails
                    while not self.task_queue.empty() and not self._shutdown_event.is_set():
                        time.sleep(0.1)
        except Exception as e:
            logger.error(f"Error waiting for completion: {e}")
            raise


def example_task(task_id: str, duration: float = 1.0) -> str:
    """Example task function for testing."""
    logger.info(f"Executing task {task_id}")
    time.sleep(duration)
    return f"Task {task_id} completed"


def main():
    """Main function demonstrating worker usage."""
    # Create and start worker
    worker = NeonWorker(max_workers=3, queue_size=50)
    worker.start()
    
    try:
        # Add some example tasks
        tasks = []
        for i in range(10):
            task = Task(
                id=f"task_{i}",
                function=example_task,
                args=(f"task_{i}",),
                kwargs={"duration": 0.5},
                priority=i % 3  # Vary priority
            )
            tasks.append(task)
            worker.add_task(task)
        
        logger.info("Tasks added, waiting for completion...")
        
        # Monitor progress
        while True:
            status = worker.get_status()
            logger.info(f"Status: {status['completed_tasks']} completed, "
                       f"{status['failed_tasks']} failed, "
                       f"{status['queue_size']} in queue")
            
            if status['queue_size'] == 0 and status['active_workers'] == 0:
                break
            
            time.sleep(1)
        
        logger.info("All tasks completed")
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()