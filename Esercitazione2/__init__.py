from CountWord import CountWord

in_file = open("venezia72_2015.txt","r")
text = in_file.read()
in_file.close()

wordcount={}

cw = CountWord()
result = cw.word_occurence(text,wordcount)
cw.print_first_one_hundred_words(result)
