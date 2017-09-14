import sys
import os
import requests
import pandas as pd
import numpy as np
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark import HiveContext


class GoogleBooksQuery():
    
    # count number of instances
    count = 0
    
    # as there is an upper limit of daily requests sent to the Google API, rather than wasting an extra call just to get
    # the number of total results, let the class already return the results and keep track how many times it was called
    def __init__(self,phrase,maxResults=40):
        self.r = requests.get(url="https://www.googleapis.com/books/v1/volumes", params={"q":phrase,
                                        "maxResults":maxResults,"startIndex":GoogleBooksQuery.count*maxResults})
        self.result = self.r.json()
        GoogleBooksQuery.count += 1
    
    @property
    def totalItems(self):
        return self.result["totalItems"]
    

        
    def extract_information(self):
        
        result = pd.DataFrame(columns=('title', 'ISBN', 'category','rating','date'))
        item_counter = 0
        for item in self.result["items"]:
            volume_info = item["volumeInfo"]
            if volume_info.get("industryIdentifiers") is not None and volume_info.get("categories") is not None and \
                volume_info.get("publishedDate") is not None and volume_info.get("averageRating") is not None:
                result.loc[item_counter] = [volume_info["title"] ,volume_info["industryIdentifiers"][0]["identifier"],\
                volume_info["categories"][0],volume_info["averageRating"],volume_info["publishedDate"]]
                item_counter += 1
        return result
                
def get_all_books(phrase,mid_year=True):
    googlebook = GoogleBooksQuery(phrase)
    books = googlebook.extract_information()
    books['year'] = books['date']
    wrong_year = []
    while True:
        try:
            googlebook = GoogleBooksQuery(phrase)
            new_books = googlebook.extract_information()
            books = books.append(new_books,ignore_index=True)
        except KeyError as e:
            break
    for i,date in enumerate(books['date']):
        try:
            books['year'][i] = re.search(r'\d{4}',date).group()
            # substitute mid-year if publication date is not known to day
            if mid_year is True and re.search(r'\d{4}-\d{2}-\d{2}',date) is None:
                if re.search(r'\d{4}-\d{2}',date) is None:
                    books['date'][i] = date+"-07-01"
                else:
                    books['date'][i] = date+"-15"
        except AttributeError as er:
            wrong_year.append(i)
    # remove years in a wrong format
    if len(wrong_year)>0:
        books = books.drop(books.index[wrong_year])
    # calculate decades
    books['decade'] = np.floor_divide(map(int,books['year']),10)*10        
    return books

def create_google_books_table(hive_context, book_list):
    '''Function used to create a SQL table called google_books based on the query phrase
       We store the title, isbn13, category and rating'''

    schema = StructType([
        StructField("isbn13", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Rating", StringType(), True),
        StructField("Title", StringType(), True),
        StructField("Year", StringType(), True),
        StructField("Decade", StringType(), True),
    ])

    DF = hive_context.createDataFrame(book_list,schema)
    DF.registerTempTable('google_books_temp')
    results = hive_context.sql('SELECT * FROM google_books_temp limit 10')
    hive_context.sql("DROP TABLE IF EXISTS google_books")
    hive_context.sql("CREATE TABLE google_books AS SELECT * FROM google_books_temp")
    return 

def merge_google_books_good_reads(hive_context):
    """Merge the good_reads table and google_books table with the common key as isbn13.
       We average the ratings from the 2 tables"""
 
    good_reads = hive_context.table("default.good_reads")
    good_reads.registerTempTable("good_reads_temp")
    #hive_context.sql("select * from good_reads_temp").show()
    hive_context.sql("DROP TABLE IF EXISTS query_bigram_result")
    hive_context.sql('CREATE TABLE query_bigram_result AS SELECT google_books.title, google_books.isbn13, google_books.category, google_books.date, google_book.year, google_books.decade, AVG(CASE WHEN good_reads_temp.rating > 0 THEN (google_books.rating + good_reads_temp.rating)/2 ELSE google_books.rating END) as rating FROM google_books LEFT JOIN good_reads_temp ON google_books.isbn13 = good_reads_temp.isbn13 GROUP BY google_books.title,google_books.isbn13, google_books.category')
    merged_table = hive_context.sql('SELECT * from query_bigram_result')
    merged_table.show()
    return

# execute from command line
if __name__ == '__main__':
    book_list = get_all_books(sys.argv[1])
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    hive_context = HiveContext(sc)
    create_google_books_table(hive_context,book_list) 

    merge_google_books_good_reads(hive_context)
