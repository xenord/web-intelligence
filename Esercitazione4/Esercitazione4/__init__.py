__author__ = "Francesco Benetello"

import time
import math
import os
import re 
import sys
import shutil
import collections
import io
from bs4 import BeautifulSoup as bs
try:
    import urllib.request as urllib2
except ImportError:
    import urllib2

news = []

# Apriamo il file contenente le stop words
with open('stopwords.txt') as f:
	stopwords = f.read().split('\n')

# Apriamo il file contenente tutti i link
with open('ANSA_UrlList.txt') as f:
	urls = f.read().split('\n')

download_dir = 'pages'

# Se la cartella contenente le pagine html esiste
# chiedo all'utente se la vuole cancellare e ricreare
if os.path.exists(download_dir):
	while (True):
		print ("Do you wanna remove entire pages directory? Y|y|n|N")
		input = sys.stdin.readline().rstrip('\n')
		if (input == "y") | (input == "Y"):
			shutil.rmtree(download_dir)
			print ("Removed with success!")
			break
		elif (input == "n") | (input == "N"):
			print ("Continuing the script..")
			time.sleep(1)
			break
		else:
			print ("Unknown command")
			continue

if not os.path.exists(download_dir):
	os.makedirs(download_dir)

try:
	for url in urls[:50]:
		url_to_low = url.lower()
		# sostituiamo tutta la punteggiature con uno spazio
		filename = re.sub('[!"#$%&\'()*+,-./:;<=>?@\[\\\\\]^_`{|}~]', ' ', url_to_low)
		# scarichiamo la pagina solo se non è già stata scaricata in precedenza
		download_path = os.path.join(download_dir, filename)
		# Estraggo le parole senza le stopwords
		newswords = [word for word in filename.split() if word not in stopwords]
		news.append(newswords)

		if not os.path.exists(download_path):
			urllib2.urlretrieve(url, download_path)
			time.sleep(1)

except ValueError:
	print ("Something goes wrong :'(")
	raise
except urllib2.HTTPError:
	print ("Error 404\nPage not found!")
	time.sleep(2)
finally:
	lexicon = {}
	wordid = 0
	for newswords in news:
		for word in newswords:
			if word not in lexicon:
				lexicon[word] = wordid
				wordid += 1

	# rappresentiamo ogni notizia come vettore
	newsvectors = []
	for newswords in news:
		occurrences = collections.Counter(newswords)
		# per comodità, rappresentiamo la notizia con un dizionario 'id parola -> occorrenze'
		# tale dizionario può essere visto come una lista di coppie (id parola, occorrenze)
		vector = {lexicon[word]: count for word, count in occurrences.items()}
		newsvectors.append(vector)

	# calcoliamo la similarità tra notizie
	def length(vec):
		return math.sqrt(sum([count*count for count in vec.values()]))

	def dot(vec1, vec2):
		# consideriamo solo le parole in comune tra vec1 e vec2
		wordsincommon = set(vec1.keys()) & set(vec2.keys())

		prods = []
		for word in wordsincommon:
			count1 = vec1[word]
			count2 = vec2[word]
			prods.append(count1*count2)
			return sum(prods)

	def similarity(vec1, vec2):
		return dot(vec1, vec2) / (length(vec1)*length(vec2))

	# la similarità di un documento qualsiasi con sé stesso deve essere 1
	print (similarity(newsvectors[2], newsvectors[2]) == 1)

	# analizziamo la similarità di tutti i documenti con es. il documento 1337
	similarities = [(i, similarity(newsvectors[1], newsvectors[i])) for i in range(len(newsvectors))]

	# i dieci documenti più simili al documento 1337 sono:
	top = sorted(similarities, reverse=True, key=lambda docid,score: score)[:10]
	print (top)