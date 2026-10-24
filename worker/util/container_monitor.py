import psutil


class ContainerMonitor:
    def __init__(self, worker_pid: int) -> None:
        self.worker_pid = worker_pid
        self.proc = psutil.Process(worker_pid)
        self.net_stat_path = f"/proc/{worker_pid}/net/dev"

    def __get_process_network_usage(self) -> tuple[int, int]:
        """Cumulative RX/TX bytes of the process's network namespace, excluding loopback.

        Loopback is skipped because the worker ships snapshot deltas to its own
        snapshotting process over localhost, which is not network traffic.
        """
        try:
            with open(self.net_stat_path) as f:  # noqa: PTH123
                lines = f.read().splitlines()[2:]  # Skip headers
        except FileNotFoundError:
            return 0, 0  # Process might have exited
        net_rx, net_tx = 0, 0
        for line in lines:
            iface, _, counters = line.partition(":")
            if iface.strip() == "lo":
                continue
            parts = counters.split()
            net_rx += int(parts[0])  # Received bytes
            net_tx += int(parts[8])  # Transmitted bytes
        return net_rx, net_tx

    def get_stats(self) -> tuple[float, int, int, int]:
        """Retrieve container stats: CPU %, memory bytes (RSS), cumulative network RX/TX bytes."""
        cpu_usage = self.proc.cpu_percent()
        memory_usage = self.proc.memory_info().rss
        net_rx, net_tx = self.__get_process_network_usage()

        return round(cpu_usage, 2), memory_usage, net_rx, net_tx
