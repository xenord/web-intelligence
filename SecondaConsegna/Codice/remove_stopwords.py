import time
import os
import re
import codecs
import io
import sys
import collections
from datetime import datetime

delete_list = ["_", "-", "@", "&amp;", "!","?",".",",","--","…","—","|"]
start_time = datetime.now()

with io.open('stopwords_en.txt', encoding='utf-8') as f:
	stopwords = f.read().split('\n')


no_stop_words = []
with codecs.open('merge.txt', encoding='utf-8', mode='r', errors='ignore')as f:
	array = []
	for line in f:
		line_lowered = line.lower()
		array.append(line_lowered)

for i in array:
	s = ""
	for j in i.split():
		if j not in stopwords:
			if j in delete_list:
				j = ""
			s += j 
			s += " "
	no_stop_words.append(s)
	no_stop_words.append("\n")

with open("stopwords_removed.txt", 'w') as f:
	for values in no_stop_words:
		f.write(values)

end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))