import re
import sys
import os
import requests
import pandas as pd
import numpy as np

# as Google limits the API results to 40, we need to call the GoogleBooksQuery multiple times to get all of the results.
# The get_all_books() function should do that.
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



# execute from command line
if __name__ == '__main__':
    result = get_all_books(sys.argv[1])
    result.to_csv(sys.argv[2],sep="\t",encoding="utf-8",header=False,index=False)
    print result
