# Neon Worker - Synchronization Solution

A robust, thread-safe background worker implementation that addresses common synchronization issues in multi-threaded environments.

## Problem Solved

This implementation resolves several critical synchronization problems:

### 1. **Race Conditions**
- **Problem**: Multiple threads accessing shared resources simultaneously can cause data corruption
- **Solution**: Implemented `SynchronizedCounter` with RLock and proper locking mechanisms in all shared data structures

### 2. **Task Queue Thread Safety** 
- **Problem**: Concurrent access to task queues can lead to lost tasks or corrupted queue state
- **Solution**: Uses Python's thread-safe `queue.PriorityQueue` with proper timeout handling

### 3. **Worker Pool Management**
- **Problem**: Uncontrolled thread creation and destruction can cause resource exhaustion
- **Solution**: Controlled worker pool using `ThreadPoolExecutor` with configurable limits

### 4. **Graceful Shutdown**
- **Problem**: Abrupt shutdown can leave tasks incomplete or resources in inconsistent state
- **Solution**: Implemented signal handlers and coordinated shutdown with proper cleanup

### 5. **Resource Contention**
- **Problem**: Multiple workers competing for shared resources without coordination
- **Solution**: Strategic use of RLock for reentrant locking and timeout mechanisms

## Key Features

### Thread-Safe Components
- **SynchronizedCounter**: Thread-safe counter with atomic operations
- **Priority Task Queue**: Thread-safe task queuing with priority support
- **Status Tracking**: Thread-safe task status monitoring
- **Configuration Management**: Thread-safe configuration updates

### Synchronization Mechanisms
- **RLock (Reentrant Lock)**: Allows same thread to acquire lock multiple times
- **Event Objects**: For coordinating shutdown across threads  
- **Queue.PriorityQueue**: Built-in thread-safe priority queue
- **ThreadPoolExecutor**: Managed thread pool with proper lifecycle

### Error Handling
- Isolated error handling per task (one failing task doesn't affect others)
- Comprehensive logging with thread identification
- Retry mechanisms with configurable parameters
- Graceful degradation under high load

## Usage Examples

### Basic Usage
```python
from neon_worker import NeonWorker, Task

# Create worker with 4 threads
worker = NeonWorker(max_workers=4, queue_size=100)
worker.start()

# Define a task function
def process_data(data_id, processing_time=1.0):
    time.sleep(processing_time)
    return f"Processed {data_id}"

# Add tasks with priority
task = Task(
    id="task_1",
    function=process_data,
    args=("data_123",),
    kwargs={"processing_time": 0.5},
    priority=1  # Higher priority = higher number
)

worker.add_task(task)

# Monitor progress
status = worker.get_status()
print(f"Completed: {status['completed_tasks']}")

# Graceful shutdown
worker.shutdown(timeout=30.0)
```

### Configuration Management
```python
from neon_worker import get_config, update_config

# Get current configuration
config = get_config()
print(f"Max workers: {config.max_workers}")

# Update configuration (thread-safe)
update_config(
    max_workers=8,
    queue_size=200,
    retry_attempts=5
)
```

### High-Concurrency Example
```python
import threading
from concurrent.futures import ThreadPoolExecutor

worker = NeonWorker(max_workers=8, queue_size=500)
worker.start()

def add_tasks_batch(batch_id, num_tasks):
    """Add multiple tasks from different threads safely"""
    for i in range(num_tasks):
        task = Task(
            id=f"batch_{batch_id}_task_{i}",
            function=your_processing_function,
            args=(f"data_{i}",),
            priority=batch_id  # Different batch priorities
        )
        worker.add_task(task)

# Add tasks from multiple threads concurrently
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = []
    for batch_id in range(5):
        future = executor.submit(add_tasks_batch, batch_id, 100)
        futures.append(future)
    
    # Wait for all batches to be submitted
    for future in futures:
        future.result()

# Wait for all processing to complete
worker.wait_for_completion()
worker.shutdown()
```

## Architecture

### Core Components

1. **NeonWorker**: Main worker class managing the entire system
2. **Task**: Encapsulates work units with metadata
3. **SynchronizedCounter**: Thread-safe counting operations  
4. **ConfigManager**: Thread-safe configuration management

### Synchronization Strategy

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Task Queue    │    │   Worker Pool    │    │  Status Monitor │
│  (PriorityQueue)│    │ (ThreadPoolExec) │    │ (Synchronized)  │
│                 │    │                  │    │                 │
│ ┌─────────────┐ │    │ ┌──────────────┐ │    │ ┌─────────────┐ │
│ │Task│Task│Task│ │────┤ │Worker Thread │ │────┤ │Shared State │ │
│ └─────────────┘ │    │ │   (RLock)    │ │    │ │  (RLock)    │ │
│                 │    │ └──────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────────┐
                    │  Shutdown Handler   │
                    │    (Event-based)    │
                    └─────────────────────┘
```

## Testing

Run the comprehensive synchronization tests:

```bash
python test_synchronization.py
```

Tests cover:
- Thread-safe counter operations
- Concurrent task execution
- Priority handling under load
- Resource contention scenarios
- Error isolation
- Graceful shutdown behavior
- Configuration thread safety
- High-concurrency stress testing

## Configuration Options

| Setting | Default | Description |
|---------|---------|-------------|
| `max_workers` | 4 | Maximum number of worker threads |
| `queue_size` | 100 | Maximum task queue size |
| `shutdown_timeout` | 30.0 | Seconds to wait for graceful shutdown |
| `log_level` | "INFO" | Logging level |
| `retry_attempts` | 3 | Number of retry attempts for failed tasks |
| `retry_delay` | 1.0 | Delay between retry attempts |
| `health_check_interval` | 10.0 | Health monitoring interval |
| `enable_metrics` | true | Enable performance metrics |

## Thread Safety Guarantees

### Locks Used
- **RLock**: Reentrant locks for nested critical sections
- **Queue.PriorityQueue**: Built-in thread-safe queue implementation
- **Threading.Event**: For shutdown coordination

### Thread-Safe Operations
- ✅ Adding tasks to queue
- ✅ Task execution and status updates
- ✅ Configuration changes
- ✅ Status monitoring
- ✅ Worker pool management
- ✅ Graceful shutdown

### Performance Considerations
- Minimal lock contention through strategic locking
- Lock-free operations where possible
- Efficient priority queue implementation
- Configurable worker pool sizing
- Timeout-based operations to prevent deadlocks

## License

MIT License - see LICENSE file for details.