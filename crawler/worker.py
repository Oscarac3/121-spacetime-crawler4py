import time
from threading import Thread

from .scraper import Scraper
from .frontier import ThreadedFrontier
from utils import get_logger, download, Config

class ThreadedWorker(Thread): # Worker must inherit from Thread or Process.
    def __init__(self, worker_id : int, config : Config, frontier : ThreadedFrontier, scraper : Scraper):
        '''
        worker_id -> a unique id for the worker to self identify.
        config -> Config object (defined in utils/config.py L1)
                  Note that the cache server is already defined at this
                  point.
        frontier -> Frontier object created by the Crawler. Base reference
                  is shown in utils/frontier.py L10 but can be overloaded
                  as detailed above.
        '''
        self.worker_id = worker_id
        self.config = config
        self.frontier = frontier
        self.scraper = scraper
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.active = True
        super().__init__(daemon=True)

    def stop(self):
        self.active = False

    def run(self):
        '''
        In loop:
            > url = get one undownloaded link from frontier.
            > resp = download(url, self.config)
            > next_links = scraper(url, resp)
            > add next_links to frontier
            > sleep for self.config.time_delay
        '''
        while self.active:
            # blocks until a URL is ready (politeness handled by Frontier)
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Worker.")
                break
            resp = download(tbd_url, self.config, self.logger)
            self.logger.debug(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped = self.scraper.scrape(tbd_url, resp)
            for link in scraped:
                self.frontier.add_url(link.url.url, link.score)
            self.frontier.mark_url_complete(tbd_url)

# class Worker(Thread):
#     def __init__(self, worker_id, config : Config, frontier : Frontier):
#         self.logger = get_logger(f"Worker-{worker_id}", "Worker")
#         self.config = config
#         self.frontier = frontier
#         super().__init__(daemon=True)
        
#     def run(self):
#         while True:
#             tbd_url = self.frontier.get_tbd_url()
#             if not tbd_url:
#                 self.logger.info("Frontier is empty. Stopping Crawler.")
#                 break
#             resp = download(tbd_url, self.config, self.logger)
#             self.logger.info(
#                 f"Downloaded {tbd_url}, status <{resp.status}>, "
#                 f"using cache {self.config.cache_server}.")
#             scraped_urls = scraper.scraper(tbd_url, resp) #TODO: Fix
#             for scraped_url in scraped_urls:
#                 self.frontier.add_url(scraped_url)
#             self.frontier.mark_url_complete(tbd_url)
#             time.sleep(self.config.time_delay)