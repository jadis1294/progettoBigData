#!/usr/bin/env python
import sys

PDIFF2016 = 0
PDIFF2017 = 1
PDIFF2018 = 2
NAME = 3
SECTOR = 4
PDIFF2016INT = 0
PDIFF2017INT = 1
PDIFF2018INT = 2
p_PercentChangeTriplet = None

'''
[dict1, dict2, ..]
where dict_i has the following schema:
{name: companyName, sector: companySector}
'''
companyList = []


def appendToList():
    listLength = len(companyList)
    for i in range(listLength - 1):
        for j in range(i, listLength):
            firstCompany = companyList[i]
            secondCompany = companyList[j]
            if firstCompany['sector'] != secondCompany['sector']:
                print('{}\t{}\t2016: {}%\t2017: {}%\t2018: {}%'
                      .format(firstCompany['name'],secondCompany['name'], p_PercentChangeTriplet[PDIFF2016INT],
                              p_PercentChangeTriplet[PDIFF2017INT],p_PercentChangeTriplet[PDIFF2018INT])
                )



def addToList(sector, name):
    entry = {'sector': sector, 'name': name}
    companyList.append(entry)


def parseValues(valueList):
    perChange2016 = valueList[PDIFF2016].strip()
    perChange2017 = valueList[PDIFF2017].strip()
    perChange2018 = valueList[PDIFF2018].strip()
    name = valueList[NAME].strip()
    sector = valueList[SECTOR].strip()
    return ((perChange2016, perChange2017, perChange2018),name,sector)


# main 
for line in sys.stdin:
    valueList = line.strip().split('\t')

    if len(valueList) == 5:
        percentChangeTriplet, name, sector = parseValues(valueList)

        if p_PercentChangeTriplet and  p_PercentChangeTriplet != percentChangeTriplet:
           
            appendToList()

            p_PercentChangeTriplet = percentChangeTriplet

            companyList = []
            addToList(sector, name)

        else:
           
            p_PercentChangeTriplet = percentChangeTriplet
            addToList(sector, name)


if p_PercentChangeTriplet:
    appendToList()
