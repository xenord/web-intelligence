# -*- coding: utf-8 -*-

__author__ = "Francesco Benetello"
import sys
import copy
import json
from string import atoi
from datetime import datetime
from pyspark import SparkContext, SparkConf
from collections import defaultdict
from itertools import imap, combinations
from Apriori import Apriori

### Definisco PATH, SUPPORTO
PATH = '/Users/francescobenetello/Documents/Dataset/sample.txt'
SUPPORTO = 0.01
##############################################


def get_minimun_occurence(supporto,dataset_len):
    return (supporto*(dataset_len))





if __name__ == "__main__":

    # Creo un oggetto Spark
    sc = SparkContext()

    # Apro RDD
    rdd = sc.textFile(PATH)

    # Ottengo il numero di partizioni in cui spark suddivide il proprio lavoro
    numPartitions = rdd.getNumPartitions()

    # Cerco la lunghezza del rdd
    lun = rdd.count()

    # Calcolo il numero minimo di occorrenze totale e per partizione
    minsup = get_minimun_occurence(SUPPORTO, lun)



    # sc, rdd, min_sup_per_partitions, minsup
    apriori_main = Apriori(sc, rdd, numPartitions, minsup)
    ciao = apriori_main.apriori()

