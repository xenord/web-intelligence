# coding=utf-8
__author__ = "Francesco Benetello"

from pyspark import SparkContext, SparkConf
import pyspark

sc = SparkContext()

textfile = sc.textFile("venezia72_2015.txt")

words = textfile.flatMap(lambda line: line.split(' '))

occurrences = words.map(lambda word: (word, 1)) \
	.reduceByKey(lambda v1, v2: v1+v2)

frequentoccurrences = occurrences.filter(lambda (word, count): count >= 2)

# Assegnamo un id univoco ad ogni tweet
tweetstoid = pyspark.RDD.zipWithUniqueId(textfile)

# associamo a ogni parola l'id del tweet in cui è contenuta

wordstotweetid = tweetstoid.flatMap(
	lambda (tweet, tweetid): [(word, tweetid) for word in tweet.split(' ') if word != ''])


# Costruiamo l'indice invertito
invertedindex = wordstotweetid.join(frequentoccurrences) \
	.map(lambda (word, (tweetid, count)): (word, tweetid)) \
	.groupByKey()

for (word, tweetids) in invertedindex.take(3):
	print 'La parola "%s" è contenuta nei tweet %s' % (word.encode('utf-8'), list(tweetids))

frequentpairs = invertedindex.cartesian(invertedindex) \
	.filter(lambda ((word1, tweetids1), (word2, tweetids2)): word1 < word2) \
	.map(lambda ((word1, tweetids1), (word2, tweetids2)):
		(word1, word2, set(tweetids1) & set(tweetids2))) \
	.filter(lambda (word1, word2, commontweetids): len(commontweetids) >= 100)

print frequentpairs.take(3)