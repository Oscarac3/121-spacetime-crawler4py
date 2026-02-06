import re
from utils import Response
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urldefrag


def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url : str, resp : Response):
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
    if resp.status != 200 or resp.raw_response is None:
        return []
    try:
        bs_obj = BeautifulSoup(resp.raw_response.content, "lxml")
    except Exception:
        return []
    
    total_links = set()
    for a in bs_obj.find_all("a", href = True):
        hlink =  a["href"]    
        total_href = urljoin(url, hlink)
        total_href, frag = urldefrag(total_href)
        total_links.add(total_href)

    return list(total_links)

def is_valid(url : str):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        
        # Only allowed domains
        allowed_domains = ["ics.uci.edu",
                        "cs.uci.edu",
                        "informatics.uci.edu",
                        "stat.uci.edu"]
        l_domain = parsed.netloc.lower()
        if not any(l_domain.endswith("." + valid_domain) or l_domain == valid_domain for valid_domain in allowed_domains):
            return False

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
