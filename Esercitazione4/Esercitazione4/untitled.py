import io      
import os                                                                                       
import re                                                                                       
import string
import collections

with io.open('stopwords.txt', encoding='utf-8') as f:   
	content = f.read()



stopwords = content.split('\n')

# leggiamo e "ripuliamo" le notizie ANSA                                                                                  

news = []                                                                                       
# ho salvato le notizie in una cartella di nome "repository" (nella directory corrente)         
news_dir = 'repository'

if not os.path.exists(news_dir):
	os.makedirs(news_dir)

for filename in os.listdir(news_dir):
	news_filepath = os.path.join(news_dir, filename)
	# processiamo la notizia corrente
	with io.open(news_filepath, encoding='utf-8') as f:
		content = f.read()                                                                  
                                                                                                 
		# convertiamo la notizia in lowercase (per evitare che es. "Venezia" e "venezia" siano viste come parole differenti)
		text = content.lower()                             

    	# sostituiamo la punteggiatura con uno spazio
		textcleaned = re.sub('[!"#$%&\'()*+,-./:;<=>?@\[\\\\\]^_`{|}~]', ' ', text)

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