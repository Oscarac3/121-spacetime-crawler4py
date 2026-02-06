import re
import data_collector
from utils import Response
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urldefrag

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
        self.url = url
        self.parsed = urlparse(url)
        self.page = self.__get_page()
        self.subdomain = self.__get_subdomain()

    def __get_page(self):
        return self.parsed.path
    
    def __get_subdomain(self):
        return self.parsed.netloc

class Scraper:

    def __init__(self):
        # includes all the urls we have seen so far (with fragments), to avoid crawling the same url twice.
        self.seen_urls = set()
        # stores the raw content hash (lecture 11) of the pages we have seen, to detect exact duplicates with different urls.
        self.seen_exact_content_hashes = set()
        # stores simhash (lecture 11) with a threshold of 90% similarity, to detect similar pages with different urls.
        self.seen_near_content_hashes = set()
        
        # Stats for report

        # Number of unique pages found (not including fragments)
        self.unique_pages = 0
        # Top 50 most common words across all pages (after removing stop words: https://www.ranks.nl/stopwords)
        self.word_freq = {}
        stop_words = {
                        "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", 
                        "any", "are", "aren't", "as", "at", "be", "because", "been", "before", 
                        "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", 
                        "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", 
                        "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", 
                        "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", 
                        "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", 
                        "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", 
                        "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", 
                        "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", 
                        "only", "or", "other", "ought", "our", "ourselves", "out", "over", 
                        "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", 
                        "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", 
                        "theirs", "them", "themselves", "then", "there", "there's", "these", "they", 
                        "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", 
                        "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", 
                        "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", 
                        "when's", "where", "where's", "which", "while", "who", "who's", "whom", 
                        "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", 
                        "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
                }
        # Subdomains found and their frequencies (needs to be converted to an alphabetically sorted list of "subdomain, frequency" for the report)
        self.subdomain_freq = {}
        pass

    def scraper(self, url : str, resp : Response):
        links = self.extract_next_links(url, resp)
        return [link for link in links if self.is_valid(link)]

    #TODO
    def detect_trap(self, url : str, resp : Response):
        '''
        Detect and avoid infinite traps. 
        For example, a calendar page that has links to the next day, which has links to the next day, and so on.
        Return True if you think this is a trap, False otherwise.
        '''
        # low words, calenders, large paths
        try:
            bs_obj = BeautifulSoup(resp.raw_response.content, "lxml")
        except Exception:
            return True
        
        page_text = bs_obj.get_text(" ", strip = True)
        if len(page_text.split()) < 75:
            return True
        
        parsed_url = urlparse(url.lower())
        path = parsed_url.path
        query = parsed_url.query
        if any(trap in path for trap in ["calendar", 'events'] ):
            return True 
        if any(trap in query for trap in ["day=", "month=", "year="]):
            return True

        if path.count("/") > 6:
            return True
        #Passed all tests 
        return False 



    #TODO
    def detect_similar(self, url : str, resp : Response):
        '''
        Detect and avoid sets of similar pages. 
        For example, a page that has links to the same page with different parameters, but the content is the same.
        We can keep an internal hashset of the content of the pages we have seen and compare.

        Return True if you think this is a similar page, False otherwise.
        '''
        pass

    #TODO
    def detect_large(self, url : str, resp : Response):
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

    #TODO
    def extract_next_links(self, url : str, resp : Response):
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
        
        #TODO:
        '''
        1. Detect and avoid dead URLs that return a 200 status but no data (DONE; Untested)
        '''

        # ----------------- Our code starts here -----------------
        if resp.status != 200 or resp.raw_response is None:
            return []
        
        # idk what this code is doing
        try:
            bs_obj = BeautifulSoup(resp.raw_response.content, "lxml")
            clean_text = bs_obj.get_text(separator=" ")
            data_collector.update_longest_page(url, clean_text)
        except Exception as e:
            print(f"Error processing {url}: {e}")
            return []
        
        total_links = set()
        for a in bs_obj.find_all("a", href = True):
            hlink =  a["href"]    
            total_href = urljoin(url, hlink)
            total_href, frag = urldefrag(total_href)
            total_links.add(total_href)

        # ----------------- Our code ends here -----------------

        return list(total_links)

    def is_valid(self, url : str):
        # Decide whether to crawl this url or not. 
        # If you decide to crawl it, return True; otherwise return False.
        # There are already some conditions that return False.
        try:
            parsed = urlparse(url)
            if parsed.scheme not in set(["http", "https"]):
                return False
            
            #TODO:
            '''
            2. Detect and avoid infinite traps (detect_trap)
            3. Detect and avoid sets of similar pages with no information (detect_similar)
            4. Detect and avoid crawling very large files, especially if they have low information value (detect_large)
            '''

            # ----------------- Our code starts here -----------------

            # Only allowed domains
            allowed_domains = ["ics.uci.edu",
                            "cs.uci.edu",
                            "informatics.uci.edu",
                            "stat.uci.edu"]
            l_domain = parsed.netloc.lower()
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
                + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

        except TypeError:
            print ("TypeError for ", parsed)
            raise
