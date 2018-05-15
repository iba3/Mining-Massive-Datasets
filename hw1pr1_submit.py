
import os
import re
import sys
import itertools
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

## Note: The above code was suggested online to link SPARK with PYCHARM ##
#print(adjlist.count())
adjlist= adjlist.filter(lambda x: x)
#print(adjlist.count())
#print("This is # of lines in file: {}".format(adjlist.count()))

def intoutput(line):

        linelist=line.split("\t")
        tuple1=int(linelist[0])
        friend_stringlist=linelist[1].split(',')

        if linelist[1]:
            friend_intlist= [int(i) for i in friend_stringlist]
            tuple2=friend_intlist
            return (tuple1,tuple2)
        else:
            return(tuple1,[-1])

f = adjlist.map(intoutput)
#print(f.take(3))



# Filtering out users with no friends
no_friends=f.filter(lambda x: x[1][0] == -1)
no_friends_output=no_friends.mapValues(lambda x: "")

#Finding all mutual pairs
fpairs=f.mapValues(lambda x: list(itertools.combinations(x, 2)))
#print(fpairs.take(1))


#Flattening all pairs so it can be reduced
fpairs_all = fpairs.flatMap(lambda x: (x[1]))
fpairs_allcount = fpairs_all.map(lambda x: (x,1))

def rearrange(x):

    if x[0][0] < x[0][1]:
        return (x[0],x[1])
    else:
        tupleval= (x[0][1],x[0][0])
        entry = x[1]
        return (tupleval,entry)



fpairs_reduced = fpairs_allcount.reduceByKey(lambda x,y: x+y)

#print("reduced by key",fpairs_reduced.collect())

##Mapping rearranging min and max and then reducing again
fpairs_reduced = fpairs_reduced.map(rearrange)
fpairs_reduced = fpairs_reduced.reduceByKey(lambda x,y: x+y)

#print("reduced by key",fpairs_reduced.collect())


## Going to be subtracting fpairs
def zipfriends(line):

    pairs = list(map(lambda x: (min(line[0], x), max(line[0], x)), line[1]))
    return pairs


#is distinct needed
fsubtract = f.flatMap(zipfriends)
fsubtract= fsubtract.map(lambda key: (key,1)).distinct()


#Subtract by key

mf_count = fpairs_reduced.subtractByKey(fsubtract)
mf_count.persist()

#print("This is mf count",mf_count.collect())

mf_count1 = mf_count.map(lambda x: (x[0][0],(x[0][1],x[1])))
mf_count2 = mf_count.map(lambda x: (x[0][1],(x[0][0],x[1])))


#print("mf_count",mf_count1.collect())
#print("mf_count2",mf_count2.collect())

mf_all = mf_count1.union(mf_count2)

mf_all.persist()

#print("mf_all",mf_all.collect())

mf_grouped = mf_all.groupByKey()

#print("mf_grouped",mf_grouped.collect())

mf_group_list = mf_grouped.mapValues(list)

#print("mg grouped list", mf_group_list.collect())

joined_sorted = mf_group_list.mapValues(lambda y: sorted(y, key=lambda x: (-x[1],x[0])))

#print("joined_sorted list", joined_sorted.collect())

top_10 = joined_sorted.mapValues(lambda x: x[:10])


#top_10 = top_10.flatMapValues(lambda x: x)
#top_10_test=top_10.filter(lambda x: x[0] in [9019,9993]).takeOrdered(10, key = lambda x: (-x[1][1],x[1][0]))
#print(top_10_test)


#print("This is mf count",top_10.collect())


def recommend(line):

    output = []
    #print("line0",line[0][0])
    for i in line:
        #print(i[0])
        listio = i[0]
        output.append(listio)

    output=', '.join(str(x) for x in output)
    #print("output",output)
    return output

top_10 = top_10.mapValues(recommend)

#print("top 10", top_10.collect())

final=top_10.union(no_friends_output)
final = final.filter(lambda x: x[0] in [924,8941,8942,9019,9020,9021,9022,9990,9992,9993])

final = final.map(lambda x: "{} \t {}".format(x[0], x[1]))


final.saveAsTextFile("./output/submit")
