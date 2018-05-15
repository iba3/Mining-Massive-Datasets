import os
import re
import sys
import itertools
import math
from operator import add
import numpy as np
import scipy as sp
import scipy.linalg
import matplotlib.pyplot as plt
from matplotlib import pyplot

os.environ['SPARK_HOME']="/Users/ife/Documents/Software/spark-2.2.1-bin-hadoop2.7"
sys.path.append("/Users/ife/Documents/Software/spark-2.2.1-bin-hadoop2.7/python/lib/")


print("hello")


## Note: The below code was suggested online to link SPARK with PYCHARM ##

try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    print("Successfully imported Spark Modules")

except ImportError as e:
    print("Can not import Spark Modules", e)
    sys.exit(1)

conf = SparkConf()
sc = SparkContext(conf=conf)

p = 123457
numbuckets = 10000

#data_words = './input/words_stream_tiny.txt'
#data_counts = './input/counts_tiny.txt'
data_words = './input/words_stream.txt'
data_counts = './input/counts.txt'

data_hash = './input/hash_params.txt'

hash_coefs = np.loadtxt(data_hash, delimiter="\t")

counts_rdd = sc.textFile(data_counts).map(lambda line: [int(x) for x in line.split('\t')])

counts_rdd = counts_rdd.map(lambda line: (line[0],line[1]))

counts_dict = counts_rdd.collectAsMap()

n = counts_rdd.count()

print("n is ", n)
print(counts_rdd.take(2))
#print("counts_dict",counts_dict.items())

for i,j in hash_coefs:
    print(i,j)

def hash_fun(hash_params, p, n_buckets, x):
    
    hash_vals = []
    for a,b in hash_params:
        y = x % p
        hash_val = (a*y + b) % p
        hash_val = (hash_val % n_buckets)+1

        hash_vals.append(hash_val)

    return hash_vals


#initialize dictionary

keys = list(range(1,n+1))
Adict =dict.fromkeys(keys,0)
Bdict =dict.fromkeys(keys,0)
Cdict =dict.fromkeys(keys,0)
Ddict =dict.fromkeys(keys,0)
Edict =dict.fromkeys(keys,0)


with open(data_words) as f:
    for count,line in enumerate(f):

        wordid = int(line)

        hash_vals = hash_fun(hash_coefs,p,numbuckets,wordid)

        #if count > 780000:
            #print("wordid",wordid)
            #print("hash_vals",hash_vals)

        Adict[hash_vals[0]] += 1
        Bdict[hash_vals[1]] += 1
        Cdict[hash_vals[2]] += 1
        Ddict[hash_vals[3]] += 1
        Edict[hash_vals[4]] += 1

        #print(wordid)

        if count % 500000==0:
            print(count)

Freqdict = {}

"finding min"
for i in range(1,n+1):
    if i%10000 ==0:
        print(i)
    hash_vals = hash_fun(hash_coefs, p, numbuckets, i)

    comparelist = []
    for entry,dictio in enumerate([Adict,Bdict,Cdict,Ddict,Edict]):

        comparelist.append(dictio[hash_vals[entry]])

    Freqdict[i] = min(comparelist)


t = count+1

Err = []
xval = []
for i in range(1,n+1):
    if i%10000 ==0:
        print(i)

    val = (Freqdict[i]-counts_dict[i])/counts_dict[i]
    Err.append(val)
    xval.append(counts_dict[i]/t)


#plt.plot(xval, Err)
#plt.show()
#pyplot.yscale('log')

fig = plt.figure()
ax = fig.add_subplot(111)
ax.set_xscale("log")
ax.set_yscale("log")
#ax.set_xlim(1e-10, 1e0) # <-- check this as pointed out by @tillsten
#ax.set_ylim(1e-5, 1e8) # <--
ax.set_aspect(1)
ax.set_title("Error vs Frequency")
ax.set_xlabel("Word Frequency")
ax.set_ylabel("Relative Error")
ax.plot(xval, Err, "k.")
ax.set_xticks([1e-8,1e-7, 1e-6, 1e-5, 1e-4,1e-3,1e-2,1e-1,1])
fig.show()

#pyplot.plot(xval,Err)
#pyplot.xlabel('Word Frequency')
#pyplot.ylabel('Relative Error')
#pyplot.title('Error Vs. Frequency')

#pyplot.xscale('log')
#pyplot.yscale('log')

pyplot.show()

