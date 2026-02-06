import os
import shelve
from typing import List

from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize, Config
from scraper import is_valid

class Frontier(object):
    def __init__(self, config : Config, restart : bool):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded : List[str] = []
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        try:
            return self.to_be_downloaded.pop()
        except IndexError:
            return None

    def add_url(self, url : str):
        url = normalize(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            self.save[urlhash] = (url, False)
            self.save.sync()
            self.to_be_downloaded.append(url)
    
    def mark_url_complete(self, url : str):
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")

        self.save[urlhash] = (url, True)
        self.save.sync()

#TODO:
'''
Make the frontier thread safe (replace the frontier list with a priority queue (threadsafe builtin) and use some locks).
Most of it can be copy paste of above.
'''

class ThreadedFrontier:
    def __init__(self, config, restart):
        #Initializer.
        # config -> Config object (defined in utils/config.py L1)
        #           Note that the cache server is already defined at this
        #           point.
        # restart -> A bool that is True if the crawler has to restart
        #           from the seed url and delete any current progress.
        pass

    def get_tbd_url(self):
        # Get one url that has to be downloaded.
        # Can return None to signify the end of crawling.
        pass

    def add_url(self, url):
        # Adds one url to the frontier to be downloaded later.
        # Checks can be made to prevent downloading duplicates.
        pass
    
    def mark_url_complete(self, url):
        # mark a url as completed so that on restart, this url is not
        # downloaded again.
        pass