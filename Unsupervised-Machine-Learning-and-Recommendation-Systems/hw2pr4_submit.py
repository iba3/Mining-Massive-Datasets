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

conf.set("spark.executor.heartbeatInterval","3600s")


alex = './input/alex.txt'
shows = './input/shows.txt'
user_shows = './input/user-shows.txt'

R = np.loadtxt(user_shows, delimiter=" ")
shownames = np.loadtxt(shows, delimiter="\n",dtype=np.str)
print(shownames)
#R = R[:,:100]
print(R)
print('num of rows and columns', R.shape)
print(R.dtype)

#row sum

pvalues = np.sum(R,axis=1)
qvalues = np.sum(R,axis=0)

print(pvalues.shape)

psqrt = np.power(pvalues, -1/2)
ssqrt = np.power(qvalues, -1/2)


P = np.diag(psqrt)
Q = np.diag(ssqrt)


test3 = np.array([[4,9],[1,9]])

print(R[:6,0])

#Suser = np.multiply(sp.linalg.fractional_matrix_power(P, -1/2),R)
Suser = P.dot(R).dot(R.T).dot(P)
Sitem = Q.dot(R.T).dot(R).dot(Q)

Recuser = np.dot(Suser,R)
Recitem = np.dot(R,Sitem)

print("shape",Recuser.shape)

alexrowuser = Recuser[499,:100].reshape(100,1).tolist()
alexrowitem = Recitem[499,:100].reshape(100,1).tolist()

print(Recuser[499,:100].shape)
print("alexrowitem",len(alexrowitem))

top8user = sorted(range(len(alexrowuser)), key=lambda i: alexrowuser[i])[-5:]
top8item = sorted(range(len(alexrowitem)), key=lambda i: alexrowitem[i])[-5:]

print(len(top8user))
#topsuserind = Suser[499,:100].argsort()[-8:][::-1]
#topsitemind = Sitem[499,:100].argsort()[-8:][::-1]

#topruserind = alexrowuser.argsort()[-8:][::-1]
#topritemind = alexrowitem.argsort()[-8:][::-1]

#susertopval = Suser[499,topsuserind]
#sitemtopval = Sitem[499,topsitemind]

#rusertopval = Recuser[499,topruserind]
#ritemtopval = Recitem[499,topritemind]

top8userval = [alexrowuser[i] for i in top8user][::-1]
top8itemval = [alexrowitem[i] for i in top8item][::-1]

print(top8userval)
print(top8itemval)

for index in top8user[::-1]:
    print(shownames[index])

print("\n")

for index in top8item[::-1]:
    print(shownames[index])

#print(rusertopval)
#print(ritemtopval)

#sortarray = Recuser[499,1:100]
#print(Recuser[499,1:100])
#print(sortarray[::-1].sort()[:10])
#print(shows[np.ravel(topsuserind)])
#print(shows[np.ravel(topsitemind)])

#print(shownames[98])
#topsusermovie = shows[topsuserind]
#topsitemmovie = shows[topsitemind]


#topmovuser = np.sort(Recuser[99,1:100])[-5:]
#topmovitem = np.sort(Recitem[99,1:100])[-5:]




#test = R.dot(R.T)
#print(test[1,1:10])

#test3 = np.power(test3, 1/2)
#print(test3)

#Suser = sp.linalg.fractional_matrix_power(test3, 1/2)
#print("Sp",test3)

#Suser = np.power(P, -1/2).dot(R).dot(R.T).dot(np.power(P, -1/2))

#print( sp.linalg.fractional_matrix_power(test, -1/2))

print("done")


