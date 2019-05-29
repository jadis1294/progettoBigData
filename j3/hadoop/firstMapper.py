#!/usr/bin/env python

import sys
import csv

RANGE1 = 2016
RANGE2 = 2018

rangeValues = range(RANGE1, RANGE2 + 1)

tickerToMap = {}

with open('historical_stocks.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    firstLine = True

    for row in csv_reader:
        if not firstLine:
            ticker, _, name, sector, _ = row
            if sector != 'N/A':
                tickerToMap[ticker] = {'sector': sector, 'name': name}
        else:
            firstLine = False

for line in sys.stdin:
    data = line.strip().split(',') 
    if len(data) == 8:
        ticker, _, close, _, _, _, _, date = data
        
        if ticker != 'ticker':
            year = int(date[0:4])

            if year in rangeValues and ticker in tickerToMap:
                sector = tickerToMap[ticker]['sector']
                name = tickerToMap[ticker]['name']
                print('{}\t{}\t{}\t{}\t{}'.format(name,ticker,date,sector,close))
