
import os
import re
import sys
import itertools
os.environ['SPARK_HOME']="/Users/ife/Documents/Software/spark-2.2.1-bin-hadoop2.7"
sys.path.append("/Users/ife/Documents/Software/spark-2.2.1-bin-hadoop2.7/python/lib/")
#--driver-memory 8G

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

## Note: The above code was suggested online to link SPARK with PYCHARM ##

#words = sc.parallelize(["scala","java","hadoop","spark","akka"])
#print(words.count())


data = './input/browsing.txt'

baskets = sc.textFile(data)
basketswid = baskets.zipWithIndex()
basketswid = basketswid.map(lambda x: (x[1],x[0].strip().split()))
basketswidlast = basketswid.map(lambda x: (x[1],x[0]))


basketswidfm = basketswid.flatMapValues(lambda x: x)
basketswidlastfm = basketswidfm.map(lambda x: (x[1],x[0]))

print("baskets wid",basketswid.collect())
print("baskets widlastfm",basketswidlastfm.take(3))

print("baskets widfmed",basketswidfm.take(3))

#items = baskets.flatMap(lambda l: re.split(r'[\s]', l))
items = baskets.flatMap(lambda l: re.split(r'[^\w]+', l))

items = items.filter(lambda x: re.search(r"^[\w]", x))

print(items.take(1))
itemsdistinct = items.distinct()
itemsdic=itemsdistinct.zipWithIndex()
print("number of  items", items.count())
print("number of distinct items", itemsdistinct.count())
print("itemsdict",itemsdic.take(1))

itemsc = items.map(lambda x: (x,1))
print("items c (w/ ones)",itemsc.take(2))

items_count = itemsc.reduceByKey(lambda x,y: x+y)
print("items count",items_count.take(3))

freq10 = items_count.mapValues(lambda x: int(x>=100))
frequent_items = items_count.filter(lambda x: x[1]>=100)
frequent_itemsid= frequent_items.map(lambda x: x[0])

freqitemsarray = frequent_items.collect()
print("frequent_items",frequent_items.count())
print("freq10",freq10.collect())

#print(type(freqitemsarray))
#print(freqitemsarray)


freq_pairsid=frequent_items.join(basketswidlastfm)

print("freq_pairsid",freq_pairsid.take(3))


freq_pairsidfrst=freq_pairsid.map(lambda x: (x[1][1],x[0]))

print("freq_pairsidfirst",freq_pairsidfrst.take(3))


basketpairs=freq_pairsidfrst.groupByKey().mapValues(lambda x: list(x))

print("basketpairs",basketpairs.take(3))

#Finding all mutual pairs
basketpairs=basketpairs.mapValues(lambda x: list(itertools.combinations(x, 2)))

print("basketpairs",basketpairs.take(3))


basketpairs_fmed = basketpairs.flatMapValues(lambda x:x)
print("basketpairs_fmed",basketpairs_fmed.take(3))


def sortpair(pairtuple):

    pair = list(pairtuple)
    pair.sort()

    return tuple(pair)

#remove if error
basketpairs_fmed = basketpairs_fmed.mapValues(sortpair)
print("basketpairs_fmed",basketpairs_fmed.take(10))
# remove above
basketpairs_fmedidlast = basketpairs_fmed.map(lambda x: (x[1],x[0]))
print("basketpairs_fmedidlast",basketpairs_fmedidlast.take(10))

basketpairs_count = basketpairs_fmed.map(lambda x: (sortpair(x[1]),1))

print("basketpairs_count sourted",basketpairs_count.take(10))

basketpairs_allcount = basketpairs_count.reduceByKey(lambda x,y: x+y)

print("basketpairs_ allcount",basketpairs_allcount.take(3))
print("basketpairs_ allcount total number",basketpairs_allcount.count())


basketpairs_frequent = basketpairs_allcount.filter(lambda x: x[1]>=100).sortBy(lambda x: -x[1])

basketpairs_frequentids = basketpairs_allcount.map(lambda x: x[0])
basketpairs_freqarray = basketpairs_frequent.collect()
#basketpairs_dict=
print("array", basketpairs_frequentids.take(3))
print("basketpairs_frequent number",basketpairs_frequent.count())
print("basketpairs_frequent",basketpairs_frequent.take(10))

print("basketpairs_filter top 5",basketpairs_frequent.take(5))

#For each basket, look in the frequent-items table to see which of its items are frequent

def checkfreqpair(pair,freqpairarray):
    exists = pair in freqpairarray
    return exists

#basketsfreqpairs = basketpairs_fmed.filter(lambda x: checkfreqpair(x[1],basketpairs_freqarray))
#print("basket freq pairs",basketsfreqpairs.take(3))


#Has basket pairs, count, basket number
freq_pairswgroup=basketpairs_frequent.join(basketpairs_fmedidlast)
print("freqpairswgroup",freq_pairswgroup.take(3))

freq_pairswbasketonly=freq_pairswgroup.map(lambda x: (x[1][1],x[0]))
print("freqpairswbasketonly",freq_pairswbasketonly.take(3))

baskettriples=freq_pairswbasketonly.groupByKey().mapValues(lambda x: list(x))
print("baskettriples",baskettriples.take(1))

#Found help online
def extracttriples(pairslist):
    ids= set(list(sum(pairslist, ())))
    triples=[]

    potential_triples= itertools.combinations(ids,3)

    for i in potential_triples:

        vals=sorted(i)
        val1=vals[0]
        val2=vals[1]
        val3=vals[2]

        if (val1,val2) and (val1,val3) and (val2,val3) in pairslist:
            triples.append(tuple(vals))

    return triples

baskettriples=baskettriples.mapValues(extracttriples)
print("btriples",baskettriples.take(1))


baskettriples_fmed=baskettriples.flatMapValues(lambda x: x)

print(baskettriples_fmed.take(3))

baskettriples_count=baskettriples_fmed.map(lambda x: (x[1],1))
print("btriple,count",baskettriples_count.take(5))



baskettriples_allcount=baskettriples_count.reduceByKey(lambda x,y:x+y)
print(baskettriples_allcount.take(5))

freqtriples=baskettriples_allcount.filter(lambda x: x[1]>=100)
print(freqtriples.take(3))


#basketpairs_frequent = frequent pairs, freqtriples = frequent triples

print(basketpairs_frequent.take(3))

##calculating confidences

freqpairs1 = basketpairs_frequent.map(lambda x: (x[0][0],[x[0][1],x[1]]))
freqpairs2 = basketpairs_frequent.map(lambda x: (x[0][1],[x[0][0],x[1]]))

basketpairs_total=freqpairs1.union(freqpairs2)
print(basketpairs_total.take(3))

print(freqitemsarray)
freqitemsdict=dict()
for i in freqitemsarray:
    freqitemsdict[i[0]]=i[1]

print(freqitemsdict['DAI62779'])

def computeconfidence(listvals,freqitemsdict):
    oid=listvals[1][0]
    supportboth=listvals[1][1]
    currid=listvals[0]

    supportcurrid=freqitemsdict[currid]
    confidence= supportboth/supportcurrid

    return (oid,confidence)

conf_freqpairs=basketpairs_total.map(lambda x: (x[0],computeconfidence(x,freqitemsdict))).sortBy(lambda x: (-x[1][1],x[0]))

print("top 5 conf sorted",conf_freqpairs.take(10))
print("top 5 conf",conf_freqpairs.takeOrdered(5,key=lambda x: -x[1][1]))

freqpairdict=dict()

for i in basketpairs_freqarray:
    freqpairdict[i[0]]=i[1]

print("testfreqpairdict",freqpairdict[('GRO81087', 'SNA45677')])


freqtriple1 = freqtriples.map(lambda x: ((x[0][0],x[0][1]),(x[0][2],x[1])))
freqtriple2 = freqtriples.map(lambda x: ((x[0][0],x[0][2]),(x[0][1],x[1])))
freqtriple3 = freqtriples.map(lambda x: ((x[0][1],x[0][2]),(x[0][0],x[1])))

frequenttripletotal=freqtriple1.union(freqtriple2)
frequenttripletotal=frequenttripletotal.union(freqtriple3)

print(frequenttripletotal.take(2))

def computeconfidencepairs(listvals,freqpairdict):
    oid=listvals[1][0]
    supporttriple=listvals[1][1]
    currpair=listvals[0]

    supportcurrpair=freqpairdict[currpair]
    confidence= supporttriple/supportcurrpair

    return (oid,confidence)

conf_freqtriples=frequenttripletotal.map(lambda x: (x[0],computeconfidence(x,freqpairdict))).sortBy(lambda x: (-x[1][1],x[0][0],x[0][1]))
print("top 5 conf triples sorted",conf_freqtriples.take(10))
