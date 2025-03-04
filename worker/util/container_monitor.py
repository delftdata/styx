import time

class ContainerMonitor:
    def __init__(self):
        """Initialize the monitor using cgroup v2 for resource metrics."""
        self.cpu_usage_last = self._read_cpu_usage()
        self.cpu_time_last = time.time()

    @staticmethod
    def _read_cpu_usage():
        """Read the total CPU usage from cgroup v2."""
        with open("/sys/fs/cgroup/cpu.stat", "r") as f:
            usage_usec = int([line.split()[1] for line in f.readlines() if "usage_usec" in line][0])
        return usage_usec  # microseconds

    @staticmethod
    def _read_memory_usage():
        """Read the memory usage in bytes."""
        with open("/sys/fs/cgroup/memory.current", "r") as f:
            return int(f.read().strip()) / (1024 * 1024)  # Convert to MB

    @staticmethod
    def _read_network_usage():
        """Read network usage from /proc/net/dev (RX/TX in KB)."""
        with open("/proc/net/dev", "r") as f:
            lines = f.readlines()[2:]  # Skip headers
        net_rx, net_tx = 0, 0
        for line in lines:
            parts = line.split()
            net_rx += int(parts[1])  # Received bytes
            net_tx += int(parts[9])  # Transmitted bytes
        return net_rx / 1024, net_tx / 1024  # Convert to KB

    def get_stats(self):
        """Retrieve container stats: CPU %, Memory MB, Network RX/TX KB."""
        # CPU Usage
        cpu_usage_now = self._read_cpu_usage()
        elapsed_time = time.time() - self.cpu_time_last
        cpu_usage_percent = ((cpu_usage_now - self.cpu_usage_last) / 1_000_000) / elapsed_time * 100
        self.cpu_usage_last = cpu_usage_now
        self.cpu_time_last = time.time()

        # Memory & Network
        mem_usage = self._read_memory_usage()
        net_rx, net_tx = self._read_network_usage()

        return round(cpu_usage_percent, 2), round(mem_usage, 2), round(net_rx, 2), round(net_tx, 2)
