from multiprocessing import Pool
import time
import os

from .constants import LIST_CL, LIST_NBA
from .data_creation import import_wikipedia
from .parallel-tfidf import *

data_nba, valid_nba = import_wikipedia(LIST_NBA)
data_cl, valid_cl = import_wikipedia(LIST_CL)

# Merging the lists
datasets = data_nba * 100 + data_cl * 100

# Merging the lists
lst_search = list_searches_nba + list_searches_cl

def test_tfidf(data): #, searches):
    mr1 = mapper1(data) #, searches)
    rr1 = reducer1(mr1)
    mr2 = mapper2(rr1)
    rr2 = reducer2(mr2)
    mr3 = mapper3(rr2)
    rr3 = reducer3(mr3)
    mr4 = mapper4(rr3)
    
    return mr4

all_times = []
for n_worker in range(1, 6):
    with Pool(processes=n_worker) as pool:
        running_times = []
        for i in range(100, 1100, 100):
            new_data = datasets[:i]
            start_time = time.time()
            result = pool.map(parallel_tfidf, new_data)
            running_times.append(time.time() - start_time)
        all_times.append(running_times)