#%%
import numpy as np
from numba import jit
from tqdm import tqdm
import time
import math
from pyspark import SparkContext
#%%
class hash_configuration():
    def __init__(self, name, hash_size, string_size = 20, hash_funcs_num = 10):
        self.name = name
        self.string_size = string_size
        self.hash_size = hash_size
        self.hash_funcs_num = hash_funcs_num
        self.coef = np.random.randint(0,self.hash_size-1,(self.hash_funcs_num, self.string_size))

@jit(nopython=True, nogil=True, parallel=False, cache=True)
def parallel(x,coef,hash_size):
    return (x*coef).sum(axis=1)%hash_size
def hash_gen(line, conf):
    x = np.array(list(map(ord, line[0:-1])))
    return parallel(x,conf.coef,conf.hash_size)
def BloomFilter(passwords1 , passwords2 , bloom_filter_conf):
    start_time = time.time()
    bloom_filter = np.zeros(bloom_filter_conf.hash_size, dtype=bool)
    num_inserted_pass = 0
    with open(passwords1) as f:
        for line in tqdm(f):
            bloom_filter[hash_gen(line = line, conf = bloom_filter_conf)] = True        
            num_inserted_pass += 1
    duplicated_pass_num = 0
    passes_num = 0
    with open(passwords2) as f:
        for line in tqdm(f):
            passes_num += 1
            if all(bloom_filter[hash_gen(line = line, conf = bloom_filter_conf)]):
                duplicated_pass_num += 1
    end_time = time.time()
    print('Number of hash function used: ', bloom_filter_conf.hash_funcs_num)
    print('Number of duplicates detected: ', duplicated_pass_num)
    k = bloom_filter_conf.hash_funcs_num
    m = bloom_filter_conf.hash_size
    n = num_inserted_pass
    e = math.exp
    print('Probability of false positives: ', (1-e(-k*n/m))**k)
    print('Execution time: ', end_time - start_time, ' secs')
    

#%%
def duplicate(passwords1 , passwords2):
    start_time = time.time()
    sc = SparkContext('local[*]', 'Find dupicated passwords')
    pass1_rdd = sc.textFile(passwords1)
    pass1_num = pass1_rdd.distinct().count()
    pass2_rdd = sc.textFile(passwords2)
    pass2_num = pass2_rdd.distinct().count()
    dup_rdd = pass2_rdd.union(pass1_rdd).distinct()
    duplicated_num = dup_rdd.count()
    end_time = time.time()
    print('Execution time: ', end_time - start_time, ' secs')
    print('The number of exact duplicated passwords is: ', pass2_num - (duplicated_num - pass1_num))