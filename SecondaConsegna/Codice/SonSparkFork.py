from pyspark import SparkContext, SparkConf
from collections import defaultdict
from itertools import imap, combinations
from datetime import datetime
from string import atoi
import pyspark
import itertools
import json
import io
import re
import sys
import copy


### Definisco PATH, SUPPORTO
PATH = '/Users/francescobenetello/Documents/Dataset/sample.txt'
SUPPORTO = 0.01
MAX_OUTPUT_LENGTH = 50
##############################################


def get_minimun_occurence(supporto,dataset_len):
	return (supporto*(dataset_len))

def remove_duplicates(l):
    return list(set(l))

def word_occurence(list_of_list):
    wordcount = {}
    for tlist in list_of_list:
        for word in tlist:
            if word not in wordcount:
                wordcount[word] = 1
            else:
                wordcount[word] += 1

    return wordcount.items()


def getMatrix(list_of_list):
    matrix = []
    word = []
    for x in list_of_list:
        i = (len (x))
        for y in range(0, i):
            if x[y] in items_only:
                word.append(x[y])
        if word:
            matrix.append(word)
        word = []
             
    return matrix

def getFrequencies(list_of_list):
    wordcount = {}
    for x in list_of_list:
        y = tuple(x)
        if y not in wordcount:
            wordcount[y] = 1
        else:
            wordcount[y] += 1

    return wordcount.items()

def getMaxLengthOfFrequentPattern(items_as_array_no_duplicate):
    maximum = 1
    for x in items_as_array_no_duplicate:
        if len(x) > maximum:
            maximum = len(x)

    return maximum

def getFrequentItemset(list_of_list):
    wordcount = {}
    for x in items_as_array_no_duplicate:
        xs = set(x)
        for y in prova:
            ys = set(y)
            if xs <= ys:
                s = str(xs)
                if s not in wordcount:
                    wordcount[s] = 1
                else:
                    wordcount[s] += 1

    return wordcount.items()



sc = SparkContext()



rdd = sc.textFile(PATH)

lun = rdd.count()
print ("Lunghezza dataset " + str(lun) + "\n") 

minsup = get_minimun_occurence(SUPPORTO, lun)
minsup = round(minsup)
print("Occorrenze >= " + str(minsup) + "\n") 

splitted = rdd.map(lambda line: [y for y in line.strip().split(' ')])
#unique = splitted.map(set).persist()
unique = splitted.map(lambda x: list(set(x)))
prova = unique.collect()
print ('Numero partizioni: ' + str(splitted.getNumPartitions()))

items = unique.mapPartitions(word_occurence).reduceByKey(lambda a, b: a + b).filter(lambda (word, count): count >= minsup)

items_only = items.map(lambda x: x[0]).collect()
items_as_array = unique.mapPartitions(getMatrix).collect()
items_as_array.sort()
items_as_array_no_duplicate = tuple(items_as_array for items_as_array,_ in itertools.groupby(items_as_array))


#maximum = getMaxLengthOfFrequentPattern(items_as_array_no_duplicate)
final = sort.mapPartitions(getFrequentItemset).reduceByKey(lambda x,y: x).filter(lambda (word, count): count >= minsup)
itemSet = final.collect()

for x in itemSet:
    print (x)

