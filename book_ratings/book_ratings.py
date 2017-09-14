from goodreads import client
import time
import csv
gc = client.GoodreadsClient("<key>", "<key>")

with open('book_reviews_1.csv','w') as csvfile:
    bookwriter = csv.writer(csvfile, delimiter=',')
                            #quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for i in range (49631,100000):
        try:
            book = gc.book(i)
        except:
            continue
        time.sleep(1.5)
        print i
        if book.language_code == "eng":
            bookwriter.writerow([book.title.encode('utf8') ,book.isbn, book.isbn13, book.average_rating])
