# -*- coding: utf-8 -*-
import sys
import copy
import json
from string import atoi
from datetime import datetime
from pyspark import SparkContext, SparkConf
from collections import defaultdict
from itertools import imap, combinations
		
class Apriori:


    # sc, rdd, rdd, numParitions, minsup
	def __init__(self, sc, rdd, numPartitions, minsup):
		self.sc = sc
		self.rdd = rdd
		self.numPartitions = numPartitions
		self.minsup = minsup
		self.minsup_per_partition = 0


	def minsupPerPartition(self):
		self.minsup_per_partition = self.minsup / self.numPartitions


	def generate_frequent_itemset(self):
	    line_to_set = []
	    items = {}
	    final = {}
	    se = set()

	    # Conto i singleton, e casto ogni riga del dataset in set
	    for line in self.rdd:
	        transaction = set(line)
	        line_to_set.append(transaction)
	        for element in transaction:
	            if element not in items:
	                items[element] = 1
	            else:
	                items[element] += 1

	    for item, count in items.iteritems():
	        if count >= self.minsup_per_partition:
	            se.add(frozenset([item]))


	    steps=1
	    pattern = 2
	    final[pattern-1] = se
	    
	    while se != set([]) and pattern != steps:

	        se = create_candidates(se,pattern)
	        frequent_candidate = frequent_items(se,line_to_set)

	        if  frequent_candidate != set([]): 
	            final[pattern] = frequent_candidate

	        se = frequent_candidate
	        pattern += 1
	        steps += 1

	    return final.values()

	def clean_set(list_of_list):
	    wordcount = {}

	    for tlist in list_of_list:
	        for elem in tlist:
	            if elem not in wordcount:
	                wordcount[elem] = 1

	    return wordcount.items()


	def apriori(self):
		self.minsupPerPartition()
		# Split riga per riga del dataset e rimuovo candidati dal basket
		splitted = self.rdd.map(lambda line: line.strip().split(' '))
		unique = splitted.map(lambda x: list(set(x))).map(lambda x: set(x))

		self.rdd = splitted
		candidates = self.rdd.mapPartitions(self.generate_frequent_itemset)

		cleaned = candidates.mapPartitions(self.clean_set).reduceByKey(lambda a, b: a)
		itemset = cleaned.map(lambda x: x[0])

		#self.rdd = itemset
		'''
		counter = unique.mapPartitions(foo)

		final = counter.reduceByKey(lambda x, y: x + y).filter(lambda (word, count): count >= minsup)
		final = final.map(lambda (itemset, count): ", ".join([str(x) for x in itemset])+"\t("+str(count)+")")
		number_of_results = final.count()
		'''
		return itemset.collect()

		

		