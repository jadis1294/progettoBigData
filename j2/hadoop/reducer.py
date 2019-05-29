#!/usr/bin/env python
import sys

SECTOR = 0
TICKER = 1
DATE = 2
CLOSE = 3
VOLUME = 4


p_Sector = None
p_Ticker = None
p_Year = None
p_Close = 0
'''
key: year, value: dictionary {
    entireVolume: entireVolumeValue,
    closeStartingValue: closeStartingValue,
    closeFinalValue: closeFinalValue
    }
'''
yearToTrend = {}  #result

'''
key: year, value: dictionary {
    date: sumOfCloseValues
    }

where date is a string in YYYY-MM-DD format and
sumOfCloseValues is the sum of the close values of tickers which
share the same sector
'''
yearToSectorDailyClosePrice = {}

def appendToList():
    for year in sorted(yearToTrend.keys()):
        sectorTrend = yearToTrend[year]
        sectorDailyClosePrices = yearToSectorDailyClosePrice[year]
        entireVolume = sectorTrend['entireVolume']
        percentChange = (sectorTrend['closeFinalValue'] - sectorTrend['closeStartingValue'])/sectorTrend['closeStartingValue']
        averageClosePrice = getDailyCloseAverage(sectorDailyClosePrices)
        print('{}\t{}\t{}\t{}\t{}'.format(p_Sector,year,entireVolume,percentChange,averageClosePrice))


def getDailyCloseAverage(yearToDailyClosePriceMap):
    count = len(yearToDailyClosePriceMap.keys())
    closeSum = sum(yearToDailyClosePriceMap.values())
    return closeSum/count



def updateDataStructure(dataStructure, year, key, value):
    if year in dataStructure:
        if key in dataStructure[year]:
            dataStructure[year][key] += value
        else:
            dataStructure[year][key] = value
    else:
        dataStructure[year] = {}
        dataStructure[year][key] = value



def parseValues(valueList):
    sector = valueList[SECTOR].strip()
    ticker = valueList[TICKER].strip()
    date = valueList[DATE].strip()
    close = float(valueList[CLOSE].strip())
    volume = int(valueList[VOLUME].strip())
    return (sector, ticker, date, close, volume)


# main 
for line in sys.stdin:
    valueList = line.strip().split('\t')

    if len(valueList) == 5:
        sector, ticker, date, close, volume = parseValues(valueList)
        year = date[0:4]

        if p_Sector and p_Sector != sector:
            #different sector
           
            updateDataStructure(yearToTrend,p_Year,'closeFinalValue',p_Close)
            appendToList()

            yearToTrend = {}
            yearToSectorDailyClosePrice = {}
            updateDataStructure(yearToTrend,year,'closeStartingValue',close)
            updateDataStructure(yearToTrend,year,'entireVolume',volume)
            updateDataStructure(yearToSectorDailyClosePrice,year,date,close)

        else:
            #same sector
            updateDataStructure(yearToTrend,year,'entireVolume',volume)
            updateDataStructure(yearToSectorDailyClosePrice,year,date,close)

             
            if p_Ticker and p_Ticker != ticker:
                # Case 1: different ticker
                updateDataStructure(yearToTrend,p_Year,'closeFinalValue',p_Close)
                updateDataStructure(yearToTrend,year,'closeStartingValue',close)

            else:
                # Case 2: same ticker
                if not p_Ticker:
                    p_Ticker = ticker
                    updateDataStructure(yearToTrend,year,'closeStartingValue',close)

                if p_Year and p_Year != year:
                    updateDataStructure(yearToTrend,p_Year,'closeFinalValue',p_Close)

                    updateDataStructure(yearToTrend,year,'closeStartingValue',close)
                    
        p_Year = year
        p_Close = close        
        p_Sector = sector
        p_Ticker = ticker
       

if p_Sector:
    
    updateDataStructure(yearToTrend,p_Year,'closeFinalValue',p_Close)
    appendToList()
