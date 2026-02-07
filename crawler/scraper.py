import re
import threading
from typing import Set
from utils import Response
from bs4 import BeautifulSoup
from dataclasses import dataclass
from urllib.parse import urlparse, urljoin, urldefrag

from .misc import STOPWORDS

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
    
    def in_domain(self, domain : str) -> bool:
        return self.subdomain and self.subdomain.endswith(domain)

    def __get_page(self) -> str:
        '''
        Returns full URL discarding fragment only
        '''
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

    def __init__(self):
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
        if any(trap in query for trap in ["day=", "month=", "year="]):
            return True
        if path.count("/") > 6: # arbitrary threshold 
            return True
        # experimental:
        path_components = path.strip('/').split('/')
        if len(path_components) != len((set(path_components))):
            return True
        #--------
        return False 

    #TODO
    def detect_similar(self, url : URL, resp : Response) -> bool:
        '''
        Detect and avoid sets of similar pages. 
        For example, a page that has links to the same page with different parameters, but the content is the same.
        We can keep an internal hashset of the content of the pages we have seen and compare.

        Return True if you think this is a similar page, False otherwise.
        '''
        pass

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
                if token not in STOPWORDS:
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
            
            # check similarity with other pages (experimental, not implemented)
            # if self.detect_similar(url, resp):
            #   return []     
            #Check for low info, like if page is > 1MB but has less than 200 words.
            if self.detect_low_info(url, resp, word_count):
                return []

            # Analytics (single lock acquisition for all updates)
            self.update_analytics(url, words, word_count)

        except Exception as e:
            print(f"Error processing {url}: {e}")
            return []
        
        total_links : Set[Link] = set()
        for a in bs_obj.find_all("a", href = True):
            hlink =  a["href"]    
            total_href = urljoin(url.url, hlink)
            total_href, frag = urldefrag(total_href)
            total_links.add(Link(URL(total_href)))

        # ----------------- Our code ends here -----------------

        return list(total_links)

    @staticmethod
    def is_valid(url : str | Link) -> bool:
        # Decide whether to crawl this url or not. 
        # If you decide to crawl it, return True; otherwise return False.
        # There are already some conditions that return False.
        try:
            if isinstance(url, str):
                url = Link(URL(url))
            url : URL = url.url
            #We check to see if the url structure is a trap or not
            if Scraper.detect_trap(url):
                return False

            if url._parsed.scheme not in set(["http", "https"]):
                return False

            # ----------------- Our code starts here -----------------

            # Only allowed domains
            allowed_domains = ["ics.uci.edu",
                            "cs.uci.edu",
                            "informatics.uci.edu",
                            "stat.uci.edu"]
            l_domain = url._parsed.netloc.lower()
            if not any(l_domain.endswith("." + valid_domain) or l_domain == valid_domain for valid_domain in allowed_domains):
                return False

            # ----------------- Our code ends here -----------------

            return not re.match(
                r".*\.(css|js|bmp|gif|jpe?g|ico"
                + r"|png|tiff?|mid|mp2|mp3|mp4"
                + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
                + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
                + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
                + r"|epub|dll|cnf|tgz|sha1"
                + r"|thmx|mso|arff|rtf|jar|csv"
                + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", url._parsed.path.lower())

        except TypeError:
            print ("TypeError for ", url._parsed)
            raise
        