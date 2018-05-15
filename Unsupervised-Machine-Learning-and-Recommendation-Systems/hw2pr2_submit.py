
import os
import re
import sys
import itertools
import math
from operator import add
import numpy as np
os.environ['SPARK_HOME']="/Users/ife/Documents/Software/spark-2.2.1-bin-hadoop2.7"
sys.path.append("/Users/ife/Documents/Software/spark-2.2.1-bin-hadoop2.7/python/lib/")


#Using maps

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

conf.set("spark.executor.heartbeatInterval","3600s")


data = './input/data/data.txt'
c1 = './input/data/c1.txt'
c2 = './input/data/c2.txt'

c1list=[]
c2list=[]

with open('./input/data/c1.txt') as f:
    for line in f:
        entrylist=line.strip().split(" ")
        entrylist=list(map(float,entrylist))
        c1list.append(entrylist)

f.close()


with open('./input/data/c2.txt') as f2:
    for line in f2:
        entrylist=line.strip().split(" ")
        entrylist=list(map(float,entrylist))
        c2list.append(entrylist)
f2.close()

#with open('./input/data/c1.txt') as f:
 #   c1list = f.readlines()

#content = [x.strip() for x in c1list]

data = sc.textFile(data)
#c1 = sc.textFile(c1)

print(data.take(1))

database = data.map(lambda x: x.split()).map(lambda x: list(map(float,x)))

data = database

print(data.first())
print(c1list[0])
maxiter = 20


words = sc.parallelize(["scala","java","hadoop","spark","akka"])
print(words.collect())

def show(rdd):
    print(rdd)

"""
Algorithm 1 Iterative k-Means Algorithm
1: procedure Iterative k-Means
2: Select k points as initial centroids of the k clusters.
3: for iterations := 1 to MAX ITER do
    4: for each point p in the dataset do
    5: Assign point p to the cluster with the closest centroid
6: end for
7: for each cluster c do
8: Recompute the centroid of c as the mean of all the data points assigned to c
9: end for
10: end for
11: end procedure

"""


words.foreach(show)

print("this is c1list",c1list)

def sqrd_eucl(list1,list2):

    sum=0
    for i,val in enumerate(list1):
        sum+=(list2[i]-val)**2

    return sum

def manhattan(list1,list2):

    sum=0
    for i,val in enumerate(list1):
        sum+=math.fabs((list2[i]-val))

    return sum

list1=[3,4]
list2=[0,0]

print("squared euclidean",sqrd_eucl(list1,list2))
print("manhattan",manhattan(list1,list2))

c1cost = []
c2cost = []

def nearestcentroid(cdict,point):

    distances=[]

    for i,centroid in cdict.items():
        distances.append((i,sqrd_eucl(point,centroid)))

    #print("this is fullclist",full_clist)
    #print("this is point",point)

    minelement = min(distances,key=lambda x: x[1])

    return minelement

def manhattan_nearestcentroid(cdict,point):

    distances=[]

    for i,centroid in cdict.items():
        distances.append((i,manhattan(point,centroid)))

    #print("this is fullclist",full_clist)
    #print("this is point",point)

    minelement = min(distances,key=lambda x: x[1])

    return minelement

def vecsum(vec1,vec2):
    v1=np.array(vec1)
    v2=np.array(vec2)

    v3 = v1+v2
    return v3


"""This if for C1 Euclidean"""
#For loop implementation.

c1_dict={}

for i,val in enumerate(c1list):
    c1_dict[i]= val

print("c2 values \n")
for i in c1_dict.values():
    print(len(i))

c2_dict={}

for i,val in enumerate(c2list):
    c2_dict[i]= val

print("c1 values \n")
for i in c2_dict.values():
    print(len(i))

#converged = False
#numiter = 0

#while not converged and numiter<maxiter:


for numiter in range(maxiter):

    #print("dict",c1_dict)
    #print(data.take(1))
    iter1 = data.map(lambda x: (x,nearestcentroid(c1_dict,x)))

    #print("finding distance",iter1.take(1)[0][1][1])
    cost1 = iter1.map(lambda x: x[1][1])
    totcost1 = cost1.reduce(lambda x,y: x + y)
    #print("this is cost1",cost1.take(15))
    #print("this is total cost 1", totcost1)

    if c1cost and totcost1==c1cost[numiter-1]:
        print("Number of iterations to convergence: ", numiter)
        break

    c1cost.append(totcost1)
    #print("this is c1cost",c1cost)

    ## Changing order so centroid is first, point second, and distance removed
    reviter1 = iter1.map(lambda x: (x[1][0],(x[0],1)))

    #print("iter1",iter1.take(1))

    #print("reversed order",reviter1.take(1))

    # reduce by key



    reviterreduced = reviter1.reduceByKey(lambda x,y: (vecsum(x[0],y[0]),x[1]+y[1]))
    #newcentroids = reviterreduced.map(lambda x: (x[0], x[1][0]/x[1][1]))
    #print("reviterreduced",reviterreduced.collect())

    newcentroids = reviterreduced.mapValues(lambda x: x[0]/x[1])

    c1_dict = newcentroids.collectAsMap()

    #print("newcentroids",newcentroids.count())

print("Max Iterations of 20 reached. Has not converged. Total cost of C1 is ", c1cost[-1])




#This is for C2 Euclidean

# For loop implementation.

for numiter in range(maxiter):

    iter1 = data.map(lambda x: (x, nearestcentroid(c2_dict, x)))

    # print("finding distance",iter1.take(1)[0][1][1])
    cost1 = iter1.map(lambda x: x[1][1])
    totcost1 = cost1.reduce(lambda x, y: x + y)

    #print("this is cost1",cost1.take(15))
    print("this is total cost 1", totcost1)

    if c2cost and totcost1 == c2cost[numiter - 1]:
        print("Number of iterations to convergence: ", numiter)
        break

    c2cost.append(totcost1)
   # print("this is c2cost", c2cost)

    ## Changing order so centroid is first, point second, and distance removed
    reviter1 = iter1.map(lambda x: (x[1][0], (x[0], 1)))
    #print("iter1",iter1.take(1))

    #print("reversed order",reviter1.take(1))
    reviterreduced = reviter1.reduceByKey(lambda x, y: (vecsum(x[0], y[0]), x[1] + y[1]))

    #print("reversed REDUCED",reviterreduced.take(1))

    newcentroids = reviterreduced.mapValues(lambda x: np.divide(x[0],x[1]))

    #newcentroids = reviterreduced.mapValues(lambda x: (type(x[1]),x[1]))
    #print("new centr",newcentroids.take(1))

    #newcentroids = reviterreduced.map(lambda x: (x[0], x[1][0]/x[1][1]))
    #print(type(reviterreduced.take(1)[0][1][0]))
    #print(reviterreduced.take(1)[0][1][0]/100)

    c2_dict = newcentroids.collectAsMap()

print("Max Iterations of 20 reached. Has not converged. Total cost of C2 is ", c2cost[-1])

print("C1cost eucl",c1cost)
print("C2cost eucl",c2cost)




#########################---- MANHATTAN DISTANCE -------############################

c3cost=[]
c4cost=[]

"""This if for  manhattan"""
#For loop implementation.

c1_dict={}

for i,val in enumerate(c1list):
    c1_dict[i]= val

print("c2 values \n")
for i in c1_dict.values():
    print(len(i))

c2_dict={}

for i,val in enumerate(c2list):
    c2_dict[i]= val

print("c1 values \n")
for i in c2_dict.values():
    print(len(i))

for numiter in range(maxiter):

    #print("dict",c1_dict)
    #print(data.take(1))
    iter1 = data.map(lambda x: (x,manhattan_nearestcentroid(c1_dict,x)))

    #print("finding distance",iter1.take(1)[0][1][1])
    cost1 = iter1.map(lambda x: x[1][1])
    totcost1 = cost1.reduce(lambda x,y: x + y)
    #print("this is cost1",cost1.take(15))
    #print("this is total cost 1", totcost1)

    if c3cost and totcost1==c3cost[numiter-1]:
        print("Number of iterations to convergence: ", numiter)
        break

    c3cost.append(totcost1)
    #print("this is c1cost",c1cost)

    ## Changing order so centroid is first, point second, and distance removed
    reviter1 = iter1.map(lambda x: (x[1][0],(x[0],1)))

    #print("iter1",iter1.take(1))

    #print("reversed order",reviter1.take(1))

    # reduce by key



    reviterreduced = reviter1.reduceByKey(lambda x,y: (vecsum(x[0],y[0]),x[1]+y[1]))
    #newcentroids = reviterreduced.map(lambda x: (x[0], x[1][0]/x[1][1]))
    #print("reviterreduced",reviterreduced.collect())

    newcentroids = reviterreduced.mapValues(lambda x: x[0]/x[1])

    c1_dict = newcentroids.collectAsMap()

    #print("newcentroids",newcentroids.count())

print("Max Iterations of 20 reached. Has not converged. Total cost of C1 is ", c1cost[-1])




#This is for C2 manhattan

# For loop implementation.

for numiter in range(maxiter):

    iter1 = data.map(lambda x: (x, manhattan_nearestcentroid(c2_dict, x)))

    # print("finding distance",iter1.take(1)[0][1][1])
    cost1 = iter1.map(lambda x: x[1][1])
    totcost1 = cost1.reduce(lambda x, y: x + y)

    #print("this is cost1",cost1.take(15))
    print("this is total cost 1", totcost1)

    if c4cost and totcost1 == c4cost[numiter - 1]:
        print("Number of iterations to convergence: ", numiter)
        break

    c4cost.append(totcost1)
   # print("this is c2cost", c2cost)

    ## Changing order so centroid is first, point second, and distance removed
    reviter1 = iter1.map(lambda x: (x[1][0], (x[0], 1)))
    #print("iter1",iter1.take(1))

    #print("reversed order",reviter1.take(1))
    reviterreduced = reviter1.reduceByKey(lambda x, y: (vecsum(x[0], y[0]), x[1] + y[1]))

    #print("reversed REDUCED",reviterreduced.take(1))

    newcentroids = reviterreduced.mapValues(lambda x: np.divide(x[0],x[1]))

    #newcentroids = reviterreduced.mapValues(lambda x: (type(x[1]),x[1]))
    #print("new centr",newcentroids.take(1))

    #newcentroids = reviterreduced.map(lambda x: (x[0], x[1][0]/x[1][1]))
    #print(type(reviterreduced.take(1)[0][1][0]))
    #print(reviterreduced.take(1)[0][1][0]/100)

    c2_dict = newcentroids.collectAsMap()

print("Max Iterations of 20 reached. Has not converged. Total cost of C2 is ", c2cost[-1])

print("C1cost eucl",c1cost)
print("C2cost eucl",c2cost)

print("C1cost manhattan",c3cost)
print("C2cost manhattan",c4cost)



"""
test = sc.parallelize([([0,1],([1,0],3.3)),([1,1],([1,0],7.2)),([0,1],([1,0],5.3)),([1,1],([1,0],2.2))])
testrev = sc.parallelize([(1,([1,0],3.3)),(1,([1,2],1.7)),(0,([1,0],5.3)),(0,([1,0],4.7))])

testreduced = testrev.reduceByKey(lambda x,y: (vecsum(x[0],y[0]),x[1]+y[1]))
testdivide = testreduced.mapValues(lambda x: x[0]/x[1])

print(testreduced.collect())
print(testdivide.collect())

print("this is test",test.first())
print("this is testrev",testrev.collect())
print("this is testrevFM",testrevFM.collect())

print("this is iter1",iter1.first())
print(test.reduce(lambda x,y: x[1][1]+y[1][1]))
#print(iter1.reduce(lambda x,y: x[1][1]+y[1][1]))
"""