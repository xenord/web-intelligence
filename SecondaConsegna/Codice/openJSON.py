__author__ = "Francesco Benetello"

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.mllib.util import MLUtils
from pyspark.mllib.fpm import FPGrowth
from pyspark.sql import Row
from datetime import datetime
import json
import pyspark
import re
import sys


sc = SparkContext()
t = sc.textFile('/Users/francescobenetello/GitHub/web-intelligence/sample.json')


def f(linea):
	return json.loads(linea)["text"]

def removePunctuation(text):
    return re.sub('[^a-z| |0-9]', '', text.strip().lower())



print (type(ciao))