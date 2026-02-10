import pickle

#TODO: Parse raw stats file into the required format of data

PATH = "/home/evij/ics121/121-spacetime-crawler4py/run/deploy/raw_stats-1.pkl"

with open(PATH, "rb") as f:
    stats : dict = pickle.load(f)

print(stats)


