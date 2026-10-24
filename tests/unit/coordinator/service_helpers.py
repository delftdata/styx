"""A CoordinatorService with its networking and recovery steps stubbed out, so
tests can drive the real migration and recovery handlers."""

import asyncio
from collections import defaultdict
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock

from styx.common.tcp_networking import NetworkingManager

from coordinator.coordinator_service import CoordinatorService
from coordinator.migration_metadata import MigrationMetadata

if TYPE_CHECKING:
    from coordinator.coordinator_metadata import Coordinator


def stub_service(coordinator: Coordinator) -> CoordinatorService:
    svc = CoordinatorService.__new__(CoordinatorService)
    svc.coordinator = coordinator
    svc.migration_in_progress = False
    svc.migration_metadata = MigrationMetadata(2)
    svc.networking_locks = defaultdict(asyncio.Lock)
    svc._migration_checkpoint_task = None
    svc._deferred_migration_graph = None
    svc.unreachable_workers = []
    svc.networking = MagicMock(close_worker_connections=AsyncMock())
    svc.protocol_networking = MagicMock(
        close_worker_connections=AsyncMock(),
        close_all_connections=AsyncMock(),
        decode_message=NetworkingManager.decode_message,
    )
    svc.aio_task_scheduler = MagicMock(close=AsyncMock())
    svc._reset_after_recovery = AsyncMock()
    svc._read_migration_blob_from_s3 = MagicMock(return_value=None)
    coordinator.start_recovery_process = AsyncMock()
    coordinator.wait_cluster_healthy = AsyncMock()
    coordinator.notify_cluster_healthy = AsyncMock()
    coordinator.revert_worker_pool_to_submitted_graph = MagicMock()
    return svc


async def recover(svc: CoordinatorService) -> None:
    """Run the real recovery for one dead worker."""
    await svc._perform_recovery({MagicMock(name="dead_worker")})
