import time
from threading import Thread
from inspect import getsource

#TODO: Rework scraper to be thread safe and have global variables rather than instance.
from . import scraper, Frontier
from utils import get_logger, download, Config

class Worker(Thread):
    def __init__(self, worker_id, config : Config, frontier : Frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper.scraper(tbd_url, resp) #TODO: Fix
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)


#TODO:
'''
Make the worker thread safe.
Handle multi-threaded politeness (separate universal time delay per domain?)
'''

class ThreadedWorker(Thread): # Worker must inherit from Thread or Process.
    def __init__(self, worker_id, config, frontier):
        # worker_id -> a unique id for the worker to self identify.
        # config -> Config object (defined in utils/config.py L1)
        #           Note that the cache server is already defined at this
        #           point.
        # frontier -> Frontier object created by the Crawler. Base reference
        #           is shown in utils/frontier.py L10 but can be overloaded
        #           as detailed above.
        self.config = config
        super().__init__(daemon=True)

    def run(self):
        '''
        In loop:
            > url = get one undownloaded link from frontier.
            > resp = download(url, self.config)
            > next_links = scraper(url, resp)
            > add next_links to frontier
            > sleep for self.config.time_delay
        '''
        pass