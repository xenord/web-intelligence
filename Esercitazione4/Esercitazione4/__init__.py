__author__ = "Francesco Benetello"

import time
import os
import re 
import sys
import shutil
import collections
import io
import math
try:
    import urllib.request as urllib2
except ImportError:
    import urllib2

wordcount = {}

# Apriamo il file contenente tutti i link
with open('ANSA_UrlList.txt') as f:
	urls = f.read().split('\n')

# Apriamo il file contenenti le stopwords
with io.open('stopwords.txt', encoding='utf-8') as f:   
	content = f.read()

stopwords = content.split('\n')

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
	for url in urls[:5]:
		# sostituiamo i caratteri diversi da a-z, A-Z e 0-9 con -
		filename = re.sub('[^a-zA-Z0-9]+', '-', url)
		# scarichiamo la pagina solo se non è già stata scaricata in precedenza
		download_path = os.path.join(download_dir, filename)
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
	print ("Calcolo la similitudine dai link che sono riuscito a scaricare!")
	news = [] 
	time.sleep(2)

	if os.listdir(download_dir) != None:
		try:
			for filename in os.listdir(download_dir):
				with open(os.path.join(download_dir, filename)) as f:
					page = f.read()

					content = page.lower()

					textcleaned = re.sub('[!"#$%&\'()*+,-./:;<=>?@\[\\\\\]^_`{|}~]', ' ', content)

					# estraiamo le parole della notizia (escludendo quelle comuni)
					newswords = [word for word in textcleaned.split() if word not in stopwords]
					news.append(newswords)

			# costruiamo il lessico, mappando ogni parola ad un id differente             

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

			print (newsvectors)

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

			# (docid, score)
			score = 0
			#print (similarity(newsvectors[4], newsvectors[4]) == 1)
			similarities = [(i, similarity(newsvectors[4], newsvectors[i])) for i in range(len(newsvectors))]
			top = sorted(similarities, key=lambda docidscore: (score), reverse=True)[:10]
			#print (top)
			#print (news[4])

			# per controllare le differenze
			print (set(news[4]) - set(news[3]))

		except ValueError:
			print ("Something goes wrong :'(")
			raise

	else:
		print ("La cartella è vuota!")