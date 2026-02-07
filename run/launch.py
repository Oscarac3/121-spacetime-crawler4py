from crawler import Crawler
from argparse import ArgumentParser
from configparser import ConfigParser
from utils import Config, get_cache_server

def main(config_file : str, restart : bool):
    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)
    config.cache_server = get_cache_server(config, restart)
    crawler = Crawler(config, restart)
    crawler.start()
    crawler_stats = crawler.get_stats()
    print("CRAWLER STATS:")
    print(crawler_stats)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
