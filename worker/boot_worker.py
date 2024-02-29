import os
import multiprocessing

import uvloop

from worker.worker_service import Worker


N_THREADS = int(os.getenv('WORKER_THREADS', 1))


class BootStyx(object):

    def __init__(self):
        self.worker_threads_pool: list[multiprocessing.Process] = []

    @staticmethod
    def start_worker_thread(thread_idx: int):
        worker = Worker(thread_idx)
        uvloop.run(worker.main())

    def main(self):
        for thread_idx in range(N_THREADS):
            self.worker_threads_pool.append(
                multiprocessing.Process(
                    target=self.start_worker_thread,
                    args=(thread_idx, )
                )
            )
        for worker_thread in self.worker_threads_pool:
            worker_thread.start()
        for worker_thread in self.worker_threads_pool:
            worker_thread.join()


if __name__ == "__main__":
    boot = BootStyx()
    boot.main()
