#!/usr/bin/env python
import sys
from datetime import datetime

# constants
TOP_N = 10
TICKER = 0
CLOSE = 2
LOW = 3
HIGH = 4
VOLUME = 5


result = []  
p_Ticker = None
closeStartingValue = None
closeFinalValue = None
minLowPrice = sys.maxsize
maxHighPrice = - sys.maxsize
volumeSum = 0
countVolume = 0

#function append items into list
def appendToList():
    closeDifference = closeFinalValue - closeStartingValue
    percentage = closeDifference/closeStartingValue
    avgVolume = volumeSum/countVolume

    record = {'ticker': p_Ticker,
              'percentageChange': percentage*100,
              'minLowPrice': minLowPrice,
              'maxHighPrice': maxHighPrice,
              'avgVolume': avgVolume
              }

    result.append(record)


def parseValues(valueList):
    ticker = valueList[TICKER].strip()
    close = float(valueList[CLOSE].strip())
    low = float(valueList[LOW].strip())
    high = float(valueList[HIGH].strip())
    volume = float(valueList[VOLUME].strip())
    return [ticker, close, low, high, volume]

# main 
for line in sys.stdin:
    valueList = line.strip().split('\t')

    if len(valueList) == 6:
        ticker, close, low, high, volume = parseValues(valueList)

        if p_Ticker and p_Ticker != ticker:
           #different ticker
            appendToList()

            # update 
            closeStartingValue = close
            closeFinalValue = close
            minLowPrice = low
            maxHighPrice = high
            volumeSum = volume
            countVolume = 1

        else:
            #same ticker or first record
            if not p_Ticker:
                closeStartingValue = close

            closeFinalValue = close
            minLowPrice = min(minLowPrice, low)
            maxHighPrice = max(maxHighPrice, high)
            volumeSum += volume
            countVolume += 1
        
        p_Ticker = ticker


if p_Ticker:
    appendToList()

sortedList = sorted(result, key=lambda k: k['percentageChange'], reverse=True)

# sort
for i in range(TOP_N):
    item = sortedList[i]
    print('{}\t{}%\t{}\t{}\t{}'.format(item['ticker'],
                                       item['percentageChange'],
                                       item['minLowPrice'],
                                       item['maxHighPrice'],
                                       item['avgVolume']))