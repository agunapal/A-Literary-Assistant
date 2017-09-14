# Query Google Books API for a phrase

Running 
    
    python google_books.py "my phrase" "my_csv_file.csv"

returns the search results as a pandas data frame with title, ISBN and category columns. The first argument is the queried phrase and the second argument is the name of the csv file to save. The csv file is tab-delimited, for PostgreSQL import use DELIMITER `E'\t'` as in:


 CREATE TABLE aboriginal(isbn varchar(25), category varchar(50), date date, title varchar(255),year varchar(4),  decade varchar(4), datetime timestamp);


    COPY aboriginal FROM '/home/adam/Documents/MIDS/W205/project/w205_final_project/google_books/aboriginal.csv' DELIMITER E'\t';

SELECT COUNT(*) Total, aboriginal.category FROM aboriginal GROUP BY aboriginal.decade, aboriginal.category ORDER BY Total DESC LIMIT 10;

SELECT COUNT(*) Total, aboriginal.category INTO aboriginal_aggregate FROM aboriginal GROUP BY  aboriginal.category ORDER BY Total DESC LIMIT 10;

CREATE TABLE aboriginal_top_10 AS SELECT * FROM aboriginal INNER JOIN aboriginal_aggregate USING (category);


# database dump to Amazon RDS

First dump the data into a local file owned by the postgres user

    pg_dump dbname=w205 -f w205_dump.sql

then execute the dump to the RDS

    psql -f w205_dump.sql --host literally-03.cst8duvjneqw.us-east-1.rds.amazonaws.com --port 5432 --username dbadmin --dbname literally

and use 
   
    psql -f w205_dump.sql --host literally-03.cst8duvjneqw.us-east-1.rds.amazonaws.com --port 5432 --username dbadmin --dbname literally

    to connect to the instance.


RStudio instance:

    http://ec2-54-89-238-69.compute-1.amazonaws.com/

allows to interact with the Amazon PostgreSQL RDS instance via

    library(dplyr)
    library(RPostgreSQL)
    my_db <- src_postgres(
       dbname = "literally",
       host = "literally-03.cst8duvjneqw.us-east-1.rds.amazonaws.com",
       user = "dbadmin",
       password = "password"
    )

    #postgres@karl560:/var/lib/postgresql/9.6/postgres_dump$ psql --host literally-03.cst8duvjneqw.us-
    #east-1.rds.amazonaws.com --port 5432 --username dbadmin --dbname literally
    #Password for user dbadmin: 
    #  psql (9.6.1, server 9.5.4)
    #SSL connection (protocol: TLSv1.2, cipher: ECDHE-RSA-AES256-GCM-SHA384, bits: 256, compression: off)
    #Type "help" for help.

    #literally=> \dt
    #List of relations
    #Schema |         Name         | Type  |  Owner  
    #--------+----------------------+-------+---------
    #  public | aboriginal           | table | dbadmin
    #  public | aboriginal_aggregate | table | dbadmin
    # public | aboriginal_top_10    | table | dbadmin
    #(3 rows)

    my_db %>% tbl("aboriginal") %>% head(5)



