import time
from tqdm import tqdm
from typing import List
from .worker import ThreadedWorker
from .frontier import ThreadedFrontier
from .scraper import Scraper
from utils import get_logger, Config

class Crawler(object):
    def __init__(self, config : Config, restart : bool, frontier_factory=ThreadedFrontier, worker_factory=ThreadedWorker, scraper_factory=Scraper):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.scraper = scraper_factory()
        self.workers : List[ThreadedWorker] = []
        self.worker_factory = worker_factory

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier, self.scraper)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def view_progress(self):
        frontier = self.frontier
        with tqdm(total=frontier.total_count, unit="it") as pbar:
            last_val = 0
            while frontier.completed_count < frontier.total_count:
                # update the goal if it changed externally
                if pbar.total != frontier.total_count:
                    pbar.total = frontier.total_count
                    pbar.refresh()
                # update the bar based on the delta
                current_val = frontier.completed_count
                delta = current_val - last_val
                if delta > 0:
                    pbar.update(delta)
                    last_val = current_val                   
                time.sleep(0.1)
            pbar.update(frontier.total_count - last_val)

    def start(self):
        self.start_async()
        self.view_progress()
        self.join()

    def join(self):
        for worker in self.workers:
            worker.join()
