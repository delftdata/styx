import psutil


class ContainerMonitor:
    def __init__(self, worker_pid: int) -> None:
        self.worker_pid = worker_pid
        self.proc = psutil.Process(worker_pid)

    def __get_process_network_usage(self) -> tuple[float, float]:
        """Reads /proc/[pid]/net/dev for per-process network usage."""
        net_stat_path = f"/proc/{self.worker_pid}/net/dev"
        try:
            with open(net_stat_path) as f:  # noqa: PTH123
                lines = f.readlines()[2:]  # Skip headers
            net_rx, net_tx = 0, 0
            for line in lines:
                parts = line.split()
                net_rx += int(parts[1])  # Received bytes
                net_tx += int(parts[9])  # Transmitted bytes
            return net_rx / 1024, net_tx / 1024  # Convert to KB
        except FileNotFoundError:
            return 0, 0  # Process might have exited

    def get_stats(self) -> tuple[float, float, float, float]:
        """Retrieve container stats: CPU %, Memory MB, Network RX/TX KB."""
        cpu_usage = self.proc.cpu_percent()
        memory_usage = self.proc.memory_info().rss / (1024 * 1024)  # Convert to MB
        net_rx, net_tx = self.__get_process_network_usage()

        return (
            round(cpu_usage, 2),
            round(memory_usage, 2),
            round(net_rx, 2),
            round(net_tx, 2),
        )
