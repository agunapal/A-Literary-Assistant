import httplib2
import re
from bs4 import BeautifulSoup, SoupStrainer
import os



http = httplib2.Http()
status, response = http.request('http://storage.googleapis.com/books/ngrams/books/datasetsv2.html')
bigrams = []
aws_prefix = "aws s3 cp s3://w205-bigram/"

for link in BeautifulSoup(response, parseOnlyThese=SoupStrainer('a')):
    # find only bigrams
    if link.has_attr('href') and 'eng-all-2gram-20120701' in link['href']:
        # exclude those bigrams that do not start with two letters
        if re.search('[a-z]{2}.gz',link['href']) is not None:
            bigram_filename = re.sub('http://storage.googleapis.com/books/ngrams/books/','',link['href'])
            bigrams.append(bigram_filename)

for name in bigrams:
     aws_cli = aws_prefix + name.decode('utf-8') + " /data/bigram_data/."
     os.system(aws_cli)


