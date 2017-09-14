#!/bin/bash

rm -f good_reads.csv
for csv_file in `echo *.csv`;
  do
    cat $csv_file >> good_reads.csv
  done
