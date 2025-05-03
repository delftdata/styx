import concurrent.futures
import multiprocessing
import multiprocessing.shared_memory
import time
import os
import random

def manager_dict_test(shared_dict, num_ops):
    """Performs operations on a manager.dict."""
    for _ in range(num_ops):
        key = random.randint(0, 99)
        shared_dict[key] = os.getpid()

def shared_memory_dict_test(shared_mem, num_ops):
    """Performs operations on shared memory."""
    for _ in range(num_ops):
        key = random.randint(0, 99)
        offset = key * 4  # Assuming integer size is 4 bytes
        shared_mem.buf[offset:offset + 4] = os.getpid().to_bytes(4, byteorder='little')

def benchmark(num_processes, num_ops):
    """Benchmarks manager.dict and shared memory with concurrent.futures."""
    results = {}

    # Manager.dict benchmark
    manager = multiprocessing.Manager()
    shared_dict = manager.dict()
    for i in range(100):
        shared_dict[i] = 0

    start_time = time.time()
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [executor.submit(manager_dict_test, shared_dict, num_ops // num_processes) for _ in range(num_processes)]
        concurrent.futures.wait(futures)
    end_time = time.time()
    results["manager_dict"] = end_time - start_time

    # Shared memory benchmark
    shared_mem = multiprocessing.shared_memory.SharedMemory(create=True, size=100 * 4)  # 100 integers
    start_time = time.time()
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [executor.submit(shared_memory_dict_test, shared_mem, num_ops // num_processes) for _ in range(num_processes)]
        concurrent.futures.wait(futures)
    end_time = time.time()
    results["shared_memory"] = end_time - start_time
    shared_mem.close()
    shared_mem.unlink()

    return results

if __name__ == "__main__":
    num_processes = 2
    num_operations = 10000
    results = benchmark(num_processes, num_operations)
    print(f"Number of processes: {num_processes}")
    print(f"Number of operations: {num_operations}")
    print(f"Manager.dict time: {results['manager_dict']:.4f} seconds")
    print(f"Shared memory time: {results['shared_memory']:.4f} seconds")