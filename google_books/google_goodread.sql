
CREATE TABLE google_books (isbn13 varchar(50),category varchar(255), date date, rating float, title varchar(255), year int, decade int);

COPY google_books FROM '/home/adam/Documents/MIDS/W205/project/w205_final_project/book_ratings/aboriginal_rating.csv' DELIMITER E'\t';


CREATE TABLE good_reads (title varchar(255), isbn varchar(10), isbn13 varchar(13), rating float);


COPY good_reads FROM '/home/adam/Documents/MIDS/W205/project/w205_final_project/book_ratings/good_reads_tab.csv' DELIMITER E'\t';

CREATE TABLE google_goodreads (title varchar(255), isbn varchar(20), category varchar(255), rating float, date date, year int, decade int); 

INSERT INTO google_goodreads SELECT google_books.title, google_books.isbn13, google_books.category, AVG(CASE WHEN good_reads.rating > 0 THEN (google_books.rating + good_reads.rating)/2 ELSE google_books.rating END) as rating, google_books.date, google_books.year, google_books.decade FROM google_books LEFT JOIN good_reads ON google_books.isbn13 = good_reads.isbn13 GROUP BY google_books.title,google_books.isbn13, google_books.category, google_books.date, google_books.year, google_books.decade;

