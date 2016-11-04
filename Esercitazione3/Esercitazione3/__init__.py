__author__ = "Francesco Benetello"

import time
import os
import re 
import sys
import shutil
import collections
import io
from bs4 import BeautifulSoup as bs
from CountWord import CountWord
try:
    import urllib.request as urllib2
except ImportError:
    import urllib2

wordcount = {}

# Apriamo il file contenente tutti i link
with open('ANSA_UrlList.txt') as f:
	urls = f.read().split('\n')

# Creo un oggetto CountWord
cw = CountWord()

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
	print ("Analizzo i link che sono riuscito a scaricare!")
	time.sleep(2)
	
	if os.listdir(download_dir) != None:
		try:
			for filename in os.listdir(download_dir):
				with open(os.path.join(download_dir, filename)) as f:
					page = f.read()

				page_soup = bs(page,"lxml")
				content_corpo = page_soup.find('div', id='content-corpo')

				words = content_corpo.text.split()
				result = cw.word_occurence(words, wordcount)

			cw.print_first_one_hundred_words(result)
		except ValueError:
			print ("Something goes wrong :'(")
			raise

	else:
		print ("La cartella è vuota!")

#else:
#	print ("-------------------------------------------")
#	print ("Finish! :)")