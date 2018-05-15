import os
import re
import sys
import itertools
import math
from operator import add
import numpy as np
import scipy as sp
import scipy.linalg

os.environ['SPARK_HOME']="/Users/ife/Documents/Software/spark-2.2.1-bin-hadoop2.7"
sys.path.append("/Users/ife/Documents/Software/spark-2.2.1-bin-hadoop2.7/python/lib/")





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

#conf.set("spark.executor.heartbeatInterval","3600s")


datastring = './input/data/ratings.train.txt'


data = sc.textFile(datastring)
data = data.map(lambda line: [int(x) for x in line.split("\t")])

users = data.map(lambda l: l[0])
numusers = users.distinct().count()

items = data.map(lambda l: l[1])
numitems = items.distinct().count()

print(data.take(1))

print("Num of users", numusers)
print("num of items", numitems)

k = 20
numiter = 40
lambd = .1

#change this
lrate = .01

print(data.count())

#Q is movies

Q = np.random.uniform(low=0, high=np.sqrt(5/k), size=(numitems,k))

#P is users

P = np.random.uniform(low=0, high=np.sqrt(5/k), size=(numusers,k))

#Making Q,P rdd instead

Qrdd = items.map(lambda line: (line,np.random.uniform(low=0, high=np.sqrt(5/k), size=(1,k))))

Prdd = users.map(lambda line: (line,np.random.uniform(low=0, high=np.sqrt(5/k), size=(1,k))))

Qlist = items.collect()
Plist = users.collect()

#print(Qlist)

Qmap ={}
Pmap = {}
for i in Qlist:
    Qmap[i] = np.random.uniform(low=0, high=np.sqrt(5/k), size=(1,k))

for i in Plist:
    Pmap[i] = np.random.uniform(low=0, high=np.sqrt(5/k), size=(1,k))

print("done")
#Qmap = Qrdd.collectAsMap()
#Pmap = Prdd.collectAsMap()

print(Pmap[186])
print(Qmap[302])

E=[]

for itert in range(numiter):

    with open(datastring) as f:
        for line in f:

            row = [int(x) for x in line.split("\t")]
            #print(row)

            x = row[0]
            i = row[1]
            rxi = row[2]

            qi = Qmap[i]
            px = Pmap[x]

            errxi = float(2 * (rxi - np.dot(qi, px.T)))

            #print(qi.dtype);
            qi_update = lrate*(errxi*px - 2*lambd*qi)
            px_update = lrate*(errxi*qi - 2*lambd*px)

            qinew = qi + qi_update
            pxnew = px + px_update

            Qmap[i] = qinew
            Pmap[x] = pxnew

        f.seek(0)

        tote = 0

        for line in f:
            row = [int(x) for x in line.split("\t")]

            x = row[0]
            i = row[1]
            rxi = row[2]

            qi = Qmap[i]
            px = Pmap[x]

            #print(qi.shape)
            #print(px.shape)
            #print(np.dot(qi,px.T).shape)
            #tote += (rxi-(np.dot(qi,px.T)))**2 + lambd*(px.dot(px.T)+qi.dot(qi.T))
            tote += (rxi-(np.dot(qi,px.T)))**2

        sump = 0
        sumq = 0

        for key,val in Pmap.items():
            sump += val.dot(val.T)

        sump = lambd*sump

        for key,val in Qmap.items():
            sumq += val.dot(val.T)

        sumq = lambd*sumq

        tote += sump+sumq

        E.append(float(tote))

f.close()

print(E)