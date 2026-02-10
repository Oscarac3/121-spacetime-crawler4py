import pickle
import pathlib

class Stats:
    def __init__(self, stats_path):
        self.stats_path = stats_path
        self.stats = self.load_stats()
        self.export_stats(self.process_stats())

    def load_stats(self):
        with open(self.stats_path, "rb") as f:
            stats : dict = pickle.load(f)
        return stats
    
    def process_stats(self):
        # If already in format, return as is
        if self.stats.get("unique_pages", None) is not None:
            return self.stats
        # Otherwise, process raw stats
        stats = {}
        stats["longest_url"] = self.stats.get("longest_url", "")
        stats["longest_count"] = self.stats.get("highest_word_count", 0)
        stats["unique_pages"] = len(self.stats.get("seen_urls", set()))
        stats["subdomain_freq"] = self.get_subdomain_freq(self.stats.get("subdomain_freq", {}))
        stats["fifty_most_freq_words"] = self.get_fifty_most_freq_words(self.stats.get("word_freq", {}))
        return stats
    
    def export_stats(self, stats):
        stats_path = pathlib.Path(self.stats_path)
        # get lowest folder in path and create "processed_stats.txt" there
        output_path = stats_path.parent / "processed_stats.txt"
        print(f"Exporting stats to {output_path}")
        with open(output_path, "w") as f:
            f.write(f"Longest page word count was {stats['longest_url']} with: {stats['longest_count']}\n")
            f.write(f"Unique pages crawled: {stats['unique_pages']}\n")
            f.write("Subdomain frequencies:\n")
            for subdomain, freq in stats["subdomain_freq"]:
                f.write(f"{subdomain}, {freq}\n")
            f.write("50 most frequent words:\n")
            for word, freq in stats["fifty_most_freq_words"]:
                f.write(f"{word}, {freq}\n")
    
    def get_fifty_most_freq_words(self, word_freq : dict):
        '''
        Returns a list of tuples (word, frequency) sorted in decreasing order of frequency (top 50)
        '''
        sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
        return sorted_words[:50]
    
    def get_subdomain_freq(self, subdomain_freq : dict):
        '''
        Returns a list of tuples (subdomain, frequency) sorted alphabetically by subdomain.
        '''
        sorted_freq = sorted(subdomain_freq.items())
        return sorted_freq

if __name__ == "__main__":
    PATH = "/home/evij/ics121/121-spacetime-crawler4py/run/deploy/run-2/20260210-114122.pkl"
    stats = Stats(PATH)

