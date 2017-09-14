import re
import sys
import os
import requests
import pandas as pd
import numpy as np

# as Google limits the API results to 40, we need to call the GoogleBooksQuery multiple times to get all of the results.
# The get_all_books() function should do that.
class GoogleBooksQuery():
    '''A Google Books Query queries the Google Books API '''
    # count number of instances
    count = 0
    
    # as there is an upper limit of daily requests sent to the Google API, rather than wasting an extra call just to get
    # the number of total results, let the class already return the results and keep track how many times it was called
    def __init__(self,phrase,maxResults=40):
        '''A GoogleBooksQuery has a url request, the json result of the request and the number of times the query was
           instantiated.
         
        Parameters:
        -----------
        phrase: string, the queried expression on Google Books
        maxResults: int between 1 and 40, the maximum number of results the query should return.
        '''

        self.r = requests.get(url="https://www.googleapis.com/books/v1/volumes", params={"q":phrase,
                                        "maxResults":maxResults,"startIndex":GoogleBooksQuery.count*maxResults})
        self.result = self.r.json()
        GoogleBooksQuery.count += 1
    
      
    def extract_information(self):
        '''Get title, ISBN, category and date for a Google Books query.'''

        result = pd.DataFrame(columns=('title', 'ISBN', 'category','date'))
        item_counter = 0
        for item in self.result["items"]:
            volume_info = item["volumeInfo"]
            if volume_info.get("industryIdentifiers") is not None and volume_info.get("categories") is not None and \
                volume_info.get("publishedDate") is not None :
                result.loc[item_counter] = [volume_info["title"] ,volume_info["industryIdentifiers"][0]["identifier"],\
                volume_info["categories"][0],volume_info["publishedDate"]]
                item_counter += 1
        return result
                
def get_all_books(phrase,mid_date=True):
    '''Returns all of the results of a Google Books query.  Deletes records with non-conforming date-types from (e.g.,
    198? instead of 1983), adds decades and years.
    Parameters:
    -----------
    phrase: query Google Books for phrase
    mid_date: boolean, if true, converts the records with only a known publication year to "year-07-01", or if month is known than "year-month-15"
    Returns:
    --------
    A pandas dataframe containing title, ISBN, category and date as columns and books as rows
    '''

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
            if mid_date is True and re.search(r'\d{4}-\d{2}-\d{2}',date) is None:
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
    books['datetime'] = books['date']+" 12:00:00"      
    return books


# execute from command line
if __name__ == '__main__':
    result = get_all_books(sys.argv[1])
    result.to_csv(sys.argv[2],sep="\t",encoding="utf-8",header=False,index=False)
    print result

