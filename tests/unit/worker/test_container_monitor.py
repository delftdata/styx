"""Unit tests for worker/util/container_monitor.py"""

import os
from unittest.mock import MagicMock, mock_open, patch

import pytest

from worker.util.container_monitor import ContainerMonitor


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestContainerMonitorInit:
    @patch("worker.util.container_monitor.psutil.Process")
    def test_stores_pid(self, mock_process):
        cm = ContainerMonitor(1234)
        assert cm.worker_pid == 1234
        mock_process.assert_called_once_with(1234)


# ---------------------------------------------------------------------------
# get_stats
# ---------------------------------------------------------------------------


class TestGetStats:
    @patch("worker.util.container_monitor.psutil.Process")
    def test_returns_four_tuple(self, mock_process_cls):
        mock_proc = MagicMock()
        mock_proc.cpu_percent.return_value = 50.123
        mock_proc.memory_info.return_value = MagicMock(rss=10 * 1024 * 1024)  # 10 MB
        mock_process_cls.return_value = mock_proc

        cm = ContainerMonitor(os.getpid())

        net_dev_content = (
            "Inter-|   Receive                                                |  Transmit\n"
            " face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n"
            "    lo: 1024       10    0    0    0     0          0         0     2048       20    0    0    0     0       0          0\n"
            "  eth0: 4096       40    0    0    0     0          0         0     8192       80    0    0    0     0       0          0\n"
        )

        with patch("builtins.open", mock_open(read_data=net_dev_content)):
            cpu, mem, rx, tx = cm.get_stats()

        assert cpu == 50.12
        assert mem == 10.0
        # rx = (1024 + 4096) / 1024 = 5.0
        assert rx == 5.0
        # tx = (2048 + 8192) / 1024 = 10.0
        assert tx == 10.0

    @patch("worker.util.container_monitor.psutil.Process")
    def test_file_not_found_returns_zero_network(self, mock_process_cls):
        mock_proc = MagicMock()
        mock_proc.cpu_percent.return_value = 10.0
        mock_proc.memory_info.return_value = MagicMock(rss=5 * 1024 * 1024)
        mock_process_cls.return_value = mock_proc

        cm = ContainerMonitor(99999)

        with patch("builtins.open", side_effect=FileNotFoundError):
            cpu, mem, rx, tx = cm.get_stats()

        assert cpu == 10.0
        assert mem == 5.0
        assert rx == 0
        assert tx == 0
