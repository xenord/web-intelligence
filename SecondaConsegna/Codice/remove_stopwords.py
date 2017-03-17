import time
import os
import re
import codecs
import io
import sys
import collections
import json
from datetime import datetime

PATH_STOPWORDS = '/Users/francescobenetello/Documents/Dataset/stopwords_en.txt'
PATH_JSON = '/Users/francescobenetello/Desktop/2015-01-08_geo_en_it_10M.plain.json'
PATH_OUTPUT = '2015-01-08_geo_en_it_10M.plain.txt'
delete_list = ['amp']

START_TIME = datetime.now()

def f(linea):
	return json.loads(linea)["text"]

def removePunctuation(text):
    return re.sub('[^a-z| |0-9]', '', text.strip().lower())

def main(PATH_STOPWORDS, PATH_JSON, PATH_OUTPUT):

	with io.open(PATH_STOPWORDS, encoding='utf-8') as f:
		stopwords = f.read().split('\n')

	text_field = []
	with codecs.open(PATH_JSON, encoding='utf-8', mode='r', errors='ignore') as f:
		for x in f:
			line = json.loads(x)["text"]
			line_cleaned = removePunctuation(line)
			text_field.append(line_cleaned)

	final = []
	for x in text_field:
		s = ''
		for y in x.split():
			if y not in stopwords:
				if y not in delete_list:
					s = s + ' ' + y
		final.append(s)
		final.append('\n')


	with open(PATH_OUTPUT, 'w') as f:
		for values in final:
			f.write(values)

	END_TIME = datetime.now()
	TIME = format(END_TIME-START_TIME)
	print("Tempo di esecuzione: " + str(TIME) + " sec")

if __name__ == '__main__':
	main(PATH_STOPWORDS, PATH_JSON, PATH_OUTPUT)