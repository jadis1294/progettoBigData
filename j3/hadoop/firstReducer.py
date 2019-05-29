#!/usr/bin/env python
import sys

NAME = 0
TICKER = 1
DATE = 2
SECTOR = 3
CLOSE = 4
RANGE1 = 2016
RANGE2 = 2018
RECORD_LENGTH = 5

rangeValues = list(range(RANGE1, RANGE2 + 1))

p_Name = None
p_Ticker = None
p_Year = None
p_Sector = None
p_Close = None

'''
key: year, value: dictionary {
    closeStartingValue: closeStartingValue, 
    closeFinalValue: closeFinalValue,
    }
'''
yearToCompany = {}

# functions 
def appendToList():

    if all(str(year) in yearToCompany for year in rangeValues):

        yearToCompany_Key = yearToCompany.keys()
        listOfSquareBrackets = ['{}'] * len(yearToCompany_Key) + ['{}', '{}']
        
        formattedString = '\t'.join(listOfSquareBrackets)
        
        percentChangeMap = {year: None for year in yearToCompany_Key}

        for year in sorted(yearToCompany.keys()):
            sectorTrend = yearToCompany[year]
            closeFinalValue = sectorTrend['closeFinalValue']
            closeStartingValue = sectorTrend['closeStartingValue']
            closeDifference = closeFinalValue - closeStartingValue
            percentChange = closeDifference/closeStartingValue
            percentChangeMap[year] = int(round(percentChange*100))

        sortedMapKeys = sorted(percentChangeMap)
        sortedMapValues = [percentChangeMap[year] for year
                                        in sortedMapKeys]
        valuesToPrint = sortedMapValues + [p_Name, p_Sector]
        print(formattedString.format(*(valuesToPrint)))


def updateTrend(dataStructure, year, key, value):
    if year in dataStructure:
        if key in dataStructure[year]:
            dataStructure[year][key] += value
        else:
            dataStructure[year][key] = value
    else:
        dataStructure[year] = {}
        dataStructure[year][key] = value



def parseValues(valueList):
    name = valueList[NAME].strip()
    ticker = valueList[TICKER].strip()
    year = valueList[DATE].strip()[0:4]
    sector = valueList[SECTOR].strip()
    close = float(valueList[CLOSE].strip())
    return (name, ticker, year, sector, close)


for line in sys.stdin:
    valueList = line.strip().split('\t')

    if len(valueList) == RECORD_LENGTH:
        name, ticker, year, sector, close = parseValues(valueList)

        if p_Name and p_Name != name:
            
            updateTrend(yearToCompany,p_Year,'closeFinalValue',p_Close)
            appendToList()
            #reset
            yearToTrend = {}

            
            updateTrend(yearToCompany,year,'closeStartingValue',close)

        else:
           
            if p_Ticker and p_Ticker != ticker:
                
                updateTrend(yearToCompany,p_Year,'closeFinalValue',p_Close)
                updateTrend(yearToCompany,year,'closeStartingValue',close)

            else:
                
                if p_Year and p_Year != year:
                    updateTrend(yearToCompany,p_Year,'closeFinalValue',p_Close)
                    updateTrend(yearToCompany,year,'closeStartingValue',close)
                else:
                    if not p_Year:
                        updateTrend(yearToCompany,year,'closeStartingValue',close)
                                           
        p_Name = name
        p_Ticker = ticker
        p_Year = year
        p_Sector = sector
        p_Close = close

if p_Name:
    updateTrend(yearToCompany,p_Year,'closeFinalValue',p_Close)
    appendToList()
