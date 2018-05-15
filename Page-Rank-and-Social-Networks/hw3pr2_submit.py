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


n=1000
beta = 0.8
data = './input/data.txt'
#data = './input/data_small.txt'


data = sc.textFile(data)

print(data.take(1))

data = data.map(lambda line: [int(x) for x in line.split("\t")])

data = data.map(lambda x: (x[0],x[1]))

print(data.count())

#Removing non unique

data=data.distinct()
print(data.count())
print("data.first",data.first())

degree= data.countByKey()

print(degree[1])

M = data

# creating x and y by switching

M = M.map(lambda x: (x[1],x[0]))

print("M first", M.first())

def add_degrees(nodei,degreedict):

    output = [nodei]
    output.append(1.0/float(degreedict[nodei]))

    return output

M = M.mapValues(lambda x: add_degrees(x,degree))

print(M.first())

#print("degreedict",degree)

r = (1/n)*np.ones((n,1))

keys = list(range(1,1001))
rdict =dict.fromkeys(keys,1.0/n)
print(rdict[1000])

def add_vectdata(line,rdict):

    if len(line) == 2:
        line.append(rdict[line[0]])
    else:

        line[2] = rdict[line[0]]

    return line

def computematmul(valuelist):


    output = [x[1]*x[2] for x in valuelist]
    output = beta*sum(output)
    output += (1-beta)/n
    return output

k = 40

#iterations equation

#r = beta*r

for iter in range(40):

    M = M.mapValues(lambda x: add_vectdata(x,rdict))

    product = M.groupByKey().mapValues(list)


    product = product.mapValues(computematmul)

    rdict = product.collectAsMap()
    #print("rdict", rdict[53])

print("rdict", rdict[53])

rlist = rdict.items()

rsort = sorted(rlist, key = lambda x: x[1])

low5 = rsort[:5]
top5=rsort[-5:]

print("low 5", low5)
print("top 5",top5)
#product = M.reduceByKey(lambda x,y: (x[0]+y[0],x[1]*x[2] + y[1]*y[2],x[2]+y[2]))
#product = M.reduceByKey(lambda x,y: (np.float64(x[0])+np.float64(y[0])))



############ Part B #############


def computematmul_partb(valuelist):



    #print(valuelist[1])
    output = [x[1]*x[2] for x in valuelist]
    output = sum(output)

    return output

hdict =dict.fromkeys(keys,1.0)

#print(hdict[1000])


L = data

#print(L.first())

L = L.mapValues(lambda x: [x,1])

#print(L.first())

for k in range(40):

    Lt = L.map(lambda x: (x[1][0],[x[0],x[1][1]]))

    #print(Lt.first())

    Lt = Lt.mapValues(lambda x: add_vectdata(x, hdict))

    Lt.persist()

    product = Lt.groupByKey().mapValues(list)

    product = product.mapValues(computematmul_partb)

    adict = product.collectAsMap()
    #print("this is product before max", product.take(1))

    beta = product.max(key = lambda x: x[1])[1]
    #print(k)

    adict.update((x, y*(1/beta)) for x, y in adict.items())

    #transpose
    #print("khere")
    L = Lt.map(lambda x: (x[1][0],[x[0],x[1][1]]))
    #print("khere1")
    L = L.mapValues(lambda x: add_vectdata(x, adict))

    L.persist()
    #print("khere2")

    product = L.groupByKey().mapValues(list)
    #print("khere3")

    product = product.mapValues(computematmul_partb)
    #print("khere4")

    hdict = product.collectAsMap()

    #print("this is product before max", product.take(1))

    lambd = product.max(key = lambda x: x[1])[1]

    hdict.update((x, y*(1/lambd)) for x, y in hdict.items())
    #print(k)

print(hdict[59])
print(adict[66])

alist = adict.items()
asort = sorted(alist, key = lambda x: x[1])
low5 = asort[:5]
top5 = asort[-5:]

print("a low 5", low5)
print("a top 5",top5)


hlist = hdict.items()
hsort = sorted(hlist, key = lambda x: x[1])
low5 = hsort[:5]
top5 = hsort[-5:]

print("h low 5", low5)
print("h top 5",top5)

