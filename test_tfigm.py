from multiprocessing import Pool
import time
import os

from .constants import LIST_CL, LIST_NBA
from .data_creation import import_wikipedia
from .parallel-tfidf import mapper1, reducer1
from .parallel-tfigm import *

data_nba, valid_nba = import_wikipedia(LIST_NBA)
data_cl, valid_cl = import_wikipedia(LIST_CL)

# Merging the lists
datasets = data_nba * 100 + data_cl * 100

# Merging the lists
lst_search = list_searches_nba + list_searches_cl

def parallel_tfigm(data):
    map_result1 = mapper1(data)
    reduce_result1 = reducer1(map_result1)
    map_result2 = mapper2(reduce_result1)
    reduce_result2 = reducer2(map_result2)
    map_result3 = mapper3(reduce_result2)
    reduce_result3 = reducer3(map_result3)
    map_result4 = mapper4(reduce_result3)
    reduce_result4 = reducer4(map_result4)
    final = mapper5(reduce_result4)
    
    return final

all_times = []
for n_worker in range(1, 6):
    with Pool(processes=n_worker) as pool:
        running_times = []
        for i in range(100, 1100, 100):
            new_data = datasets[:i]
            start_time = time.time()
            result = pool.map(parallel_tfigm, new_data)
            running_times.append(time.time() - start_time)
        all_times.append(running_times)