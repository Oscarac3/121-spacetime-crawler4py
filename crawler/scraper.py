import re
import pickle
import hashlib
import threading
from typing import Set
from utils import Response, get_logger
from bs4 import BeautifulSoup
from dataclasses import dataclass
from urllib.parse import urlparse, urljoin, urldefrag

from .misc import STOPWORDS, BAD_EXT_REGEX

'''
Server Cache status codes
These are all the cache server error codes:
600: Request Malformed
601: Download Exception {error}
602: Spacetime Server Failure
603: Scheme has to be either http or https
604: Domain must be within spec
605: Not an appropriate file extension
606: Exception in parsing url
607: Content too big. {resp.headers['content-length']}
608: Denied by domain robot rules
You may ignore some of them, but not all.
'''

class URL:
    def __init__(self, url : str):
        self.url = url.lower()
        self._parsed = urlparse(self.url)
        self.page = self.__get_page()
        self.subdomain = self.__get_subdomain()

    def __hash__(self):
        return hash(self.page)
    
    def __eq__(self, other):
        if not isinstance(other, URL):
            return NotImplemented
        return self.page == other.page

    def __str__(self):
        return self.url
    
    def __repr__(self):
        return self.url
    
    def in_domain(self, domain : str) -> bool:
        return self.subdomain and self.subdomain.endswith(domain)
    
    def valid_scheme(self) -> bool:
        return self._parsed.scheme in set(["http", "https"])

    def __get_page(self) -> str:
        '''
        Returns full URL discarding fragment only
        '''
        if not self.valid_scheme(): return ""
        path = self._parsed.path
        if path.endswith("/"):
            path = path[:-1]
        base = self._parsed.scheme + "://" + self._parsed.netloc + path
        if self._parsed.query:
            base += "?" + self._parsed.query
        return base
    
    def __get_subdomain(self) -> str:
        '''
        Returns the full subdomain of the URL, including the domain.
        '''
        if not self.valid_scheme(): return ""
        return self._parsed.scheme + "://" + self._parsed.hostname
    
@dataclass
class Link:
    url: URL
    score: float = 0

    def __hash__(self):
        return self.url.__hash__()

    def __eq__(self, other):
        if not isinstance(other, Link):
            return NotImplemented
        return self.url == other.url
class Scraper:

    def __init__(self, restart : bool = False):
        self.logger = get_logger("Scraper")
        # includes all the urls we have seen so far (no fragments), to avoid crawling the same url twice.
        self.seen_urls : Set[URL] = set()
        # stores the raw content hash (lecture 11) of the pages we have seen, to detect exact duplicates with different urls.
        self.seen_exact_content_hashes = set()
        # stores simhash (lecture 11) with a threshold of 90% similarity, to detect similar pages with different urls.
        self.seen_near_content_hashes = set()
        
        # Thread safety lock
        self.lock = threading.RLock()

        # Stats for report
        # Top 50 most common words across all pages (after removing stop words: https://www.ranks.nl/stopwords)
        self.word_freq = {}
        # Subdomains found and their frequencies (needs to be converted to an alphabetically sorted list of "subdomain, frequency" for the report)
        self.subdomain_freq = {}
        #Longest page variables
        self.longest_url = None
        self.highest_word_count = 0

        if not restart:
            self.load_state()

    def load_state(self):
        '''Loads the state from a pkl file'''
        try:
            with open("raw_stats.pkl", "rb") as f:
                raw_stats : dict = pickle.load(f)
                self.seen_urls = raw_stats.get("seen_urls", set())
                self.seen_exact_content_hashes = raw_stats.get("seen_exact_content_hashes", set())
                self.seen_near_content_hashes = raw_stats.get("seen_near_content_hashes", set())
                self.word_freq = raw_stats.get("word_freq", {})
                self.subdomain_freq = raw_stats.get("subdomain_freq", {})
                self.longest_url = raw_stats.get("longest_url", None)
                self.highest_word_count = raw_stats.get("highest_word_count", 0)
            self.logger.info(f"Loaded state from raw_stats.pkl: {len(self.seen_urls)} seen URLs.")
        except FileNotFoundError:
            pass

    def scrape(self, url : str, resp : Response) -> list[Link]:
        links = self.extract_next_links(url, resp)
        return [link for link in links if self.is_valid(link)]

    def tokenize(self, text: str) -> list[str]:
        #Tokenizes the text removing non alphanumeric chars
        return re.findall(r'[a-zA-Z0-9]+', text.lower())

    @staticmethod
    def detect_trap(url : URL, resp : Response = None) -> bool:
        '''
        Detect and avoid infinite traps. 
        For example, a calendar page that has links to the next day, which has links to the next day, and so on.
        Return True if you think this is a trap, False otherwise.
        '''
        # calenders, large paths
        path = url._parsed.path
        query = url._parsed.query
        if any(trap in path for trap in ["calendar", 'events'] ):
            return True 
        if any(trap in query for trap in ["day=", "month=", "year=", "rev=", "idx=", "rev="]):
            return True
        if "do=" in query and "do=show" not in query:
            return True
        if path.count("/") > 6: # arbitrary threshold 
            return True
        if len(url.url) > 100: # arbitrary threshold
            return True
        # experimental:
        path_components = path.strip('/').split('/')
        if len(path_components) != len((set(path_components))):
            return True
        #--------
        return False 

    #TODO
    def detect_exact_similar(self, url : URL, words: list[str]) -> bool:
        '''
        Detect and avoid sets of exact pages. 
        For example, a page that has links to the same page with different parameters, but the content is the same.
        We can keep an internal hashset of the content of the pages we have seen and compare.

        Return True if you think this is a similar page, False otherwise.
        '''
        #only using built in libraries like hashlib and re 
        if not words:
            return False

        #single string from list of words, then hash
        content_string = "".join(words)
        content_hash = hashlib.sha1(content_string.encode("utf-8")).hexdigest()

        if content_hash in self.seen_exact_content_hashes:
            return True
            
        self.seen_exact_content_hashes.add(content_hash)
        return False

    def detect_near_similar(self, url: URL, words: list[str])-> bool:
        '''
        Detect and avoid sets of near similar pages. 
        For example, a page that has links to the same page with different parameters, but the content is the same.
        We can keep an internal hashset of the content of the pages we have seen and compare.

        Return True if you think this is a similar page, False otherwise.
        Using Simhash from lecture 
        '''
        if not words:
            return False

        #compute term frequencies
        freqs = {}
        for token in words:
            freqs[token] = freqs.get(token, 0) + 1

        #simhash 
        v = [0] * 64
        for token, weight in freqs.items():
            #use the hash of token to update the vector
            hash_u = int(hashlib.sha1(token.encode("utf-8")).hexdigest(), 16)
            for i in range(64):
                if (hash_u >> i) & 1:
                    v[i] += weight
                else:
                    v[i] -= weight

        simhash = 0
        for i, sum_ in enumerate(v):
            if sum_ > 0:
                simhash |= (1 << i)

        #compare Hamming distance with seen hashes
        for seen_hash in self.seen_near_content_hashes:
            hamming = bin(simhash ^ seen_hash).count("1")
            similarity = 1 - (hamming / 64)
            if similarity >= 0.95: 
                return True

        self.seen_near_content_hashes.add(simhash)
        return False


    def detect_large(self, url : URL, resp : Response) -> bool:
        '''
        Detect and avoid crawling very large files, especially if they have low information value. 
        For example, a page that has a lot of images, but no text.
        Return True if you think this is a large file, False otherwise.
        '''
        if not resp.raw_response or not resp.raw_response.content:
            return False
        
        if len(resp.raw_response.content) > 5 * 1024 * 1024: #5MB
            return True

        return False

    def detect_low_info(self, url : URL, resp : Response, word_count : int) -> bool:
        if not resp.raw_response or not resp.raw_response.content:
            return True

        file_size = len(resp.raw_response.content)
        if file_size >  1024 * 1024 and word_count < 200: #1MB
            return True

        return False

    def update_analytics(self, url : URL, words : list, word_count : int):
        """
        Atomically updates all analytics: word frequency, longest page, and subdomain counts.
        Single lock acquisition to avoid unnecessary churn.
        """
        with self.lock:
            # Word frequency
            for token in words:
                if token not in STOPWORDS and len(token) > 1 and token.isdigit() == False:
                    self.word_freq[token] = self.word_freq.get(token, 0) + 1
            # Longest page
            if word_count > self.highest_word_count:
                self.highest_word_count = word_count
                self.longest_url = url
            # Subdomain counting
            if url.in_domain("uci.edu"):
                self.subdomain_freq[url.subdomain] = self.subdomain_freq.get(url.subdomain, 0) + 1
            
    def get_uniquePages_num(self):
        '''Returns the number of unique pages we have seen so far'''
        return len(self.seen_urls)
    
    def get_longest_page(self):
        '''Returns the longest page URL and its word count'''
        return self.longest_url, self.highest_word_count
    
    def get_fifty_most_freq_words(self):
        '''
        Returns a list of tuples (word, frequency) sorted in decreasing order of frequency (top 50)
        '''
        with self.lock:
            sorted_words = sorted(self.word_freq.items(), key=lambda x: x[1], reverse=True)
        return sorted_words[:50]
    
    def get_subdomain_freq(self):
        '''
        Returns a list of tuples (subdomain, frequency) sorted alphabetically by subdomain.
        '''
        with self.lock:
            sorted_freq = sorted(self.subdomain_freq.items())
        return sorted_freq
    
    def extract_next_links(self, url : str, resp : Response) -> list[Link]:
        '''
        Implementation required.
        url: the URL that was used to get the page
        resp.url: the actual url of the page
        resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
        resp.error: when status is not 200, you can check the error here, if needed.
        resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
                resp.raw_response.url: the url, again
                resp.raw_response.content: the content of the page!
        Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
        '''
        
        '''
        1. Detect and avoid dead URLs that return a 200 status but no data (DONE; Untested)
        '''
        
        # ----------------- Our code starts here -----------------
        url : URL = URL(url)
        # Check if we have seen the page before
        with self.lock:
            if url in self.seen_urls:
                return []
            self.seen_urls.add(url)
        if resp.status != 200 or resp.raw_response is None:
            return []
        # check to see if file is too large 
        if self.detect_large(url, resp):
            return []
        # check to see if it's a trap
        if Scraper.detect_trap(url, resp):
            return []
        # Now read content and extract links
        try:
            bs_obj = BeautifulSoup(resp.raw_response.content, "lxml")
            clean_text = bs_obj.get_text(separator=" ")       

            #TOKENIZE WORDS
            words = self.tokenize(clean_text)
            word_count = len(words)
            
            # check exact similarity with other pages (experimental)
            if self.detect_exact_similar(url, words):
                return []
            # check near similarity with other pages (experimental)
            if self.detect_near_similar(url, words):
                return []     
            
            #Check for low info, like if page is > 1MB but has less than 200 words.
            if self.detect_low_info(url, resp, word_count):
                return []

            # Analytics (single lock acquisition for all updates)
            self.update_analytics(url, words, word_count)

        except Exception as e:
            self.logger.info(f"Error processing {url}: {e}")
            return []
        
        total_links : Set[Link] = set()
        for a in bs_obj.find_all("a", href = True):
            hlink =  a["href"]    
            total_href = urljoin(url.url, hlink)
            total_href, frag = urldefrag(total_href)
            try:
                total_links.add(Link(URL(total_href)))
            except Exception as e:
                self.logger.info(f"Error processing href {total_href} on page {url}: {e}")
                continue

        # ----------------- Our code ends here -----------------

        return list(total_links)

    @staticmethod
    def is_valid(url : str | Link) -> bool:
        # Decide whether to crawl this url or not. 
        # If you decide to crawl it, return True; otherwise return False.
        # There are already some conditions that return False.
        try:
            # ----------------- Our code starts here -----------------
            if isinstance(url, str):
                url = Link(URL(url))
            url : URL = url.url
            # Check if the url has a valid scheme
            if url._parsed.scheme not in set(["http", "https"]):
                return False
            #We check to see if the url structure is a trap or not
            if Scraper.detect_trap(url):
                return False
            # Only allowed domains
            allowed_domains = ["ics.uci.edu",
                            "cs.uci.edu",
                            "informatics.uci.edu",
                            "stat.uci.edu"]
            l_domain = url._parsed.netloc.lower()
            if not any(l_domain.endswith("." + valid_domain) or l_domain == valid_domain for valid_domain in allowed_domains):
                return False
            # Disallowed links
            disallowed = []
            for dis in disallowed:
                if url.url.startswith(dis):
                    return False
            # special cases
            gitlab = "https://gitlab.ics.uci.edu"
            if url.url.startswith(gitlab) and any(tag in url.url for tag in ["commit", "tags", "forks", "tree", "branches", "merge_requests", "issues"]):
                return False
            # ----------------- Our code ends here -----------------
            return not BAD_EXT_REGEX.search(url.url)

        except TypeError:
            print("TypeError for ", url._parsed)
            raise
        