#!/usr/bin/env python

import sys

RANGE_1 = 1998
RANGE_2 = 2018

rangeValues = range(RANGE_1, RANGE_2+ 1)

for line in sys.stdin:
    
    data = line.strip().split(',') 
    if len(data) == 8:
        ticker, _, close, _, low, high, volume, date = data
        
        if ticker != 'ticker':
            year = int(date[0:4])
           #check year 
            if year in rangeValues:
                print('{}\t{}\t{}\t{}\t{}\t{}'.format(ticker,date,close,low,high,volume))
