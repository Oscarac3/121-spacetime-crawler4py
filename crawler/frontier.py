import os
import shelve
from typing import List, Dict, Tuple

import time
import heapq
from urllib.parse import urlparse
from collections import defaultdict
from threading import RLock, Condition

from .scraper import Scraper
from utils import get_logger, get_urlhash, normalize, Config
class ThreadedFrontier(object):
    def __init__(self, config : Config, restart : bool):
        '''
        config -> Config object (defined in utils/config.py L1)
                  Note that the cache server is already defined at this
                  point.
        restart -> A bool that is True if the crawler has to restart
                  from the seed url and delete any current progress.
        '''
        self.logger = get_logger("Frontier")
        self.config = config
        
        self.lock = RLock()
        self.cv = Condition(self.lock)
        
        # {domain : [(value, count, url), ...]} where value is the information value and count is like a timestamp to break ties
        self.domain_queues : Dict[str, List[Tuple[int, int, str]]] = defaultdict(list)
        # min-Heap of tuples: (next_available_timestamp, domain)
        self.domain_heap = []
        self.domains_in_heap = set()
        self.entry_count = 0 

        # For viewing
        self.total_count = 0
        self.completed_count = 0

        # Termination tracking: how many workers currently have a URL checked out
        self.active_workers = 0

        self.polite = True

        self._init_frontier(restart)
    
    def _get_domain(self, url : str) -> str:
        subdomain = urlparse(url).netloc
        if self.polite:
            valid_domains = [urlparse(d).netloc for d in self.config.seed_urls]
            for valid_domain in valid_domains:
                if subdomain == valid_domain or subdomain.endswith("." + valid_domain) or subdomain.endswith(valid_domain.removeprefix("www.")):
                    return valid_domain
            self.logger.warning(f"URL {url} has subdomain {subdomain} which does not match any valid domain {valid_domains}. Assigning to {subdomain} as its own domain.")
        return subdomain

    def _init_frontier(self, restart : bool) -> None:
        if not os.path.exists(self.config.save_file) and not restart:
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            if self.save:
                self._parse_save_file()
            else:
                for url in self.config.seed_urls:
                    self.add_url(url)
    
    def _parse_save_file(self) -> None:
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        max_entry_count = 0
        with self.lock:
            for urlhash, data in self.save.items():
                if len(data) == 2:
                    url, completed = data
                else:
                    url, completed, score, count = data
                    if not completed and Scraper.is_valid(url):
                        self._add_to_memory(url, count, score)
                        tbd_count += 1
                        max_entry_count = max(max_entry_count, count)
            self.entry_count = max_entry_count
            self.total_count = total_count
            self.completed_count = total_count - tbd_count
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")
        
    def _add_to_memory(self, url, entry_count, score=0) -> None:
        ''' Internal helper to add URL to queues and heap. Assumes lock held. '''
        domain = self._get_domain(url)
        heapq.heappush(self.domain_queues[domain], (-score, entry_count, url))
        
        if domain not in self.domains_in_heap:
            heapq.heappush(self.domain_heap, (time.time(), domain))
            self.domains_in_heap.add(domain)

    def get_tbd_url(self) -> str | None:
        # Get one url that has to be downloaded.
        # Can return None to signify the end of crawling.
        with self.lock:
            while True:
                if self.domain_heap:
                    next_time, domain = self.domain_heap[0]
                    now = time.time()                
                    if now >= next_time:
                        heapq.heappop(self.domain_heap)
                        if domain in self.domain_queues and self.domain_queues[domain]:
                            _, _, url = heapq.heappop(self.domain_queues[domain])
                            new_next_time = time.time() + self.config.time_delay
                            if self.domain_queues[domain]:
                                heapq.heappush(self.domain_heap, (new_next_time, domain))
                            else:
                                self.domains_in_heap.remove(domain)
                                del self.domain_queues[domain]
                            self.active_workers += 1
                            time_since_last = int((now - (next_time - self.config.time_delay)) * 1000)
                            if time_since_last < self.config.time_delay * 1000:
                                raise ValueError(f"Dispatched URL {url} from domain {domain} after only {time_since_last} ms since last dispatch, which is less than the configured time delay of {self.config.time_delay * 1000} ms.")
                            self.logger.debug(f"Dispatching URL {url} from domain {domain} | {time_since_last} ms since last dispatch.")
                            return url
                        else:
                            if domain in self.domains_in_heap:
                                self.domains_in_heap.remove(domain)
                            continue
                    else:
                        wait_time = next_time - now
                        self.cv.wait(timeout=wait_time)
                else:
                    # Heap is empty: if no workers are active, crawl is truly done
                    if self.active_workers == 0:
                        self.cv.notify_all()
                        return None
                    # Otherwise, a worker may still produce new URLs — wait
                    self.cv.wait(timeout=1.0)

    def add_url(self, url : str, score : int = 0, entry_count : int | None = None) -> None:
        # Adds one url to the frontier to be downloaded later.
        # Checks can be made to prevent downloading duplicates.
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.save:
                if entry_count is None:
                    self.entry_count += 1
                    entry_count = self.entry_count
                self.total_count += 1
                self.save[urlhash] = (url, False, score, entry_count)
                self.save.sync()
                self._add_to_memory(url, entry_count, score)
                # notify workers
                self.cv.notify_all()
    
    def mark_url_complete(self, url : str) -> None:
        # mark a url as completed so that on restart, this url is not
        # downloaded again.
        urlhash = get_urlhash(url)
        with self.lock:
            self.completed_count += 1
            self.active_workers -= 1
            if urlhash not in self.save:
                self.logger.error(f"Completed url {url}, but have not seen it before.")
                return
            self.save[urlhash] = (url, True)
            self.save.sync()
            # Notify in case workers are waiting for termination check
            self.cv.notify_all()

# class Frontier(object):
#     def __init__(self, config : Config, restart : bool):
#         self.logger = get_logger("FRONTIER")
#         self.config = config
#         self.to_be_downloaded : List[str] = []
#         self._init_frontier(restart)

#     def _init_frontier(self, restart : bool):
#         if not os.path.exists(self.config.save_file) and not restart:
#             self.logger.info(
#                 f"Did not find save file {self.config.save_file}, "
#                 f"starting from seed.")
#         elif os.path.exists(self.config.save_file) and restart:
#             self.logger.info(
#                 f"Found save file {self.config.save_file}, deleting it.")
#             os.remove(self.config.save_file)
#         self.save = shelve.open(self.config.save_file)
#         if restart:
#             for url in self.config.seed_urls:
#                 self.add_url(url)
#         else:
#             if self.save:
#                 self._parse_save_file()
#             else:
#                 for url in self.config.seed_urls:
#                     self.add_url(url)

#     def _parse_save_file(self):
#         ''' This function can be overridden for alternate saving techniques. '''
#         total_count = len(self.save)
#         tbd_count = 0
#         for url, completed in self.save.values():
#             if not completed and is_valid(url):
#                 self.to_be_downloaded.append(url)
#                 tbd_count += 1
#         self.logger.info(
#             f"Found {tbd_count} urls to be downloaded from {total_count} "
#             f"total urls discovered.")

#     def get_tbd_url(self):
#         try:
#             return self.to_be_downloaded.pop()
#         except IndexError:
#             return None

#     def add_url(self, url : str):
#         url = normalize(url)
#         urlhash = get_urlhash(url)
#         if urlhash not in self.save:
#             self.save[urlhash] = (url, False)
#             self.save.sync()
#             self.to_be_downloaded.append(url)
    
#     def mark_url_complete(self, url : str):
#         urlhash = get_urlhash(url)
#         if urlhash not in self.save:
#             # This should not happen.
#             self.logger.error(
#                 f"Completed url {url}, but have not seen it before.")

#         self.save[urlhash] = (url, True)
#         self.save.sync()