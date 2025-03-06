from prometheus_client import start_http_server, Summary, Counter, Gauge, Histogram
import time
import random
import psutil

# Add additional metrics here if needed
REQUEST_COUNT = Counter('app_requests_total', 'Total number of requests')
REQUEST_LATENCY = Summary('app_request_latency_seconds', 'Request latency')
CURRENT_TASKS = Gauge('app_current_tasks', 'Number of ongoing tasks')
TASK_HISTOGRAM = Histogram('app_task_duration_seconds', 'Histogram of task duration')

# Basic resource metrics
cpu_usage_gauge = Gauge("container_cpu_usage_percent", "CPU usage percentage", ["instance"])
memory_usage_gauge = Gauge("container_memory_usage_mb", "Memory usage in MB", ["instance"])
network_rx_gauge = Gauge("container_network_rx_kb", "Network received KB", ["instance"])
network_tx_gauge = Gauge("container_network_tx_kb", "Network transmitted KB", ["instance"])
epoch_throughput_gauge = Gauge("worker_epoch_throughput_tps", "Epoch Throughput (transactions per second)", ["instance"])
epoch_latency_gauge = Gauge("worker_epoch_latency_ms", "Epoch Latency (ms)", ["instance"])
epoch_abort_gauge = Gauge("worker_abort_percent", "Epoch Concurrency Abort percentage", ["instance"])
# TODO: Add (combined metric) for latency components
snapshotting_gauge = Gauge("worker_total_snapshotting_time_ms", "Snapshotting time (ms)", ["instance"])

# Get number of CPUs
cpu_list = [f"{i}" for i in range(psutil.cpu_count(logical=True))]

def process_request():
    """Simulate processing a request."""
    REQUEST_COUNT.inc()  # Increment the counter
    start_time = time.time()

    duration = random.uniform(0.5, 2.0)  # Simulate variable task duration
    time.sleep(duration)

    REQUEST_LATENCY.observe(time.time() - start_time)  # Observe latency
    TASK_HISTOGRAM.observe(duration)  # Observe duration

def update_metrics():
    """Update resource usage metrics for each CPU."""
    for cpu in cpu_list:
        #cpu_index = int(cpu.split("_")[1])  # Extract CPU number
        cpu_index = int(cpu)

        # Use random values for testing
        cpu_usage_gauge.labels(instance=cpu).set(random.uniform(1, 100))
        memory_usage_gauge.labels(instance=cpu).set(random.uniform(0, 100))
        network_rx_gauge.labels(instance=cpu).set(random.uniform(0, 10000))
        network_tx_gauge.labels(instance=cpu).set(random.uniform(0, 10000))
        epoch_throughput_gauge.labels(instance=cpu).set(random.uniform(0, 10000))
        epoch_latency_gauge.labels(instance=cpu).set(random.uniform(0, 1000))
        epoch_abort_gauge.labels(instance=cpu).set(random.uniform(0, 100))
        snapshotting_gauge.labels(instance=cpu).set(random.uniform(0, 1000))

        # OR use real system stats from psutil
        cpu_percentages = psutil.cpu_percent(percpu=True)  # List of CPU usages per core
        if cpu_index < len(cpu_percentages):  # Avoid out-of-range error
            cpu_usage_gauge.labels(instance=cpu).set(cpu_percentages[cpu_index])

    # Memory & Network stats are system-wide, so they remain the same
    #memory_usage_gauge.labels(instance="system").set(psutil.virtual_memory().used / (1024 * 1024))  # Convert to MB
    #net_io = psutil.net_io_counters()
    #network_rx_gauge.labels(instance="system").set(net_io.bytes_recv / 1024)  # Convert to KB
    #network_tx_gauge.labels(instance="system").set(net_io.bytes_sent / 1024)  # Convert to KB

if __name__ == "__main__":
    start_http_server(8000)  # Expose metrics on http://localhost:8000
    print("Serving metrics on http://localhost:8000")

    while True:
        CURRENT_TASKS.set(random.randint(0, 10))  # Update Gauge with a random number
        update_metrics()  # Update CPU, memory, network stats
        process_request()
