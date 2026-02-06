from typing import List

from .worker import Worker, ThreadedWorker
from .frontier import Frontier, ThreadedFrontier
from utils import get_logger, Config

#TODO: When the worker and frontier are threadsafe, replace the factory defaults with the threaded versions.
class Crawler(object):
    def __init__(self, config : Config, restart : bool, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers : List[Worker] = []
        self.worker_factory = worker_factory

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):
        self.start_async()
        self.join()

    def join(self):
        for worker in self.workers:
            worker.join()
