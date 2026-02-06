import threading
import re

#lock to protect access to the longest page record
stats_lock = threading.Lock()

#track of the longest page found
longest_url = ""
highest_word_count = 0

def update_longest_page(url, text):
    """
    Counts words in text & updates the longest page record if needed
    """
    global longest_url, highest_word_count

    # tokenize text into words
    words = re.findall(r'[a-zA-Z0-9]+', text)
    count = len(words)
    #update
    with stats_lock:
        if count > highest_word_count:
            highest_word_count = count
            longest_url = url
            
          #update
            print(f"\n[LONG PAGE] New Record: {count} words at {url}")

            #write to file
            with open("longest_page_result.txt", "w") as f:
                f.write(f"Longest Page: {longest_url}\n")
                f.write(f"Word Count: {highest_word_count}\n")