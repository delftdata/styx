#!/usr/bin/env python3

import multiprocessing as mp
import os

import uvloop

from worker.worker_service import Worker

N_THREADS = int(os.getenv("WORKER_THREADS", "1"))


class BootStyx:
    def __init__(self) -> None:
        self.worker_threads_pool: list[mp.Process] = []

    @staticmethod
    def start_worker_thread(thread_idx: int) -> None:
        worker = Worker(thread_idx)
        uvloop.run(worker.main())

    def main(self) -> None:
        for thread_idx in range(N_THREADS):
            self.worker_threads_pool.append(
                mp.Process(target=self.start_worker_thread, args=(thread_idx,)),
            )
        for p in self.worker_threads_pool:
            p.start()
        for p in self.worker_threads_pool:
            p.join()


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    boot = BootStyx()
    boot.main()
