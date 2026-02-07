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
            while True:
                with frontier.lock:
                    completed = frontier.completed_count
                    total = frontier.total_count
                    active = frontier.active_workers
                if completed >= total and active == 0:
                    break
                # update the goal if it changed externally
                if pbar.total != total:
                    pbar.total = total
                    pbar.refresh()
                # update the bar based on the delta
                delta = completed - last_val
                if delta > 0:
                    pbar.update(delta)
                    last_val = completed
                # also break if all worker threads have died
                if not any(w.is_alive() for w in self.workers):
                    break
                time.sleep(0.1)
            pbar.update(total - last_val)

    def start(self):
        self.start_async()
        try:
            self.view_progress()
        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt received, stopping crawler.")
        self.join()

    def join(self):
        for worker in self.workers:
            worker.stop()
            worker.join()

    def get_stats(self):
        longest_url, longest_count = self.scraper.get_longest_page()
        unique_pages = self.scraper.get_uniquePages_num()
        subdomain_freq = self.scraper.get_subdomain_freq()
        fifty_most_freq_words = self.scraper.get_fifty_most_freq_words()
        return {
            "longest_url": longest_url,
            "longest_count": longest_count,
            "unique_pages": unique_pages,
            "subdomain_freq": subdomain_freq,
            "fifty_most_freq_words": fifty_most_freq_words
        }