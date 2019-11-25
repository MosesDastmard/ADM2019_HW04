from sklearn.decomposition import PCA
from sklearn.preprocessing import scale
from scipy.spatial.distance import euclidean
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import numpy as np
#%%
sc = SparkContext("local[*]", "K-means")
#%%
rdd = sc.textFile('wine.data').\
        map(lambda x: np.array(list(map(float,x.split(','))))).\
        map(lambda x: np.reshape(x, (1,-1))).\
        reduce(lambda x,y: np.concatenate([x,y], axis=0))
data = rdd
avg = rdd.mean(axis = 0)
std = rdd.std(axis = 0)
normal = (data - avg)/std
#%%
res = rdd.collect()
