from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession,DataFrame
from pyspark.sql.functions import lit, concat, when, to_date
from pyspark.sql import functions as F
import time
def init_spark(app_name, master_config):
    """
    :params app_name: Name of the app
    :params master_config: eg. local[4]
    :returns SparkContext, SQLContext, SparkSession:
    """
    conf = SparkConf().setAppName(app_name).setMaster(master_config)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    sqlContext = SQLContext(sc)
    sparks = SparkSession(sc)
    return (sc, sqlContext, sparks)
sc, sqlContext, sparks = init_spark("Homework1", "local[4]")

startTimeQuery = time.clock()

#Load the csv without the header and create de init DataFrame with the schema

doc1= sparks.read.load( "/home/Jadis/Documenti/bigData/daily-historical-stock-prices-1970-2018/dimezzato.csv",\
       format="csv", sep=",",header=True) \
      .toDF( "ticker","open","close","adj","low","max","volume","date" )  

#Load the csv without the header and create de init DataFrame with the schema
doc2= sparks.read.load( "/home/Jadis/Documenti/bigData/daily-historical-stock-prices-1970-2018/historical_stocks.csv",\
       format="csv", sep=",",header=True) \
      .toDF( "ticker","exchange","name","sector","industry","")  

#initial join of the two csv with the column anno
init=doc1.join(doc2,"ticker")\
      .withColumn('anno', concat(doc1.date.substr(1, 4)))
#take the years>2004
doc= init.withColumn('anno',when(init.anno >= 2004, init.anno)).drop(init.anno)
initDataFrame= doc.filter(doc.anno.isNotNull())

print("finish join")
dataminmax=initDataFrame.groupBy(["anno","sector","ticker"])\
      .agg(F.max(F.to_date(initDataFrame.date)),F.min(F.to_date(initDataFrame.date)))\
      .toDF("anno","sector","ticker","max_data","min_data")

volumeComplessivo=initDataFrame.groupBy(["sector","anno"])\
      .agg(F.sum(initDataFrame.volume))\
      .toDF("sector","anno","volumeComplessivo")

sommaclose=initDataFrame.groupBy(["sector","anno","date"])\
      .agg(F.sum(initDataFrame.close))\
      .toDF("sector","anno","date","sommaclose")
print("dataminmanx")
join1= dataminmax.join(initDataFrame,["ticker","sector","anno"])\
      .where(initDataFrame.date==dataminmax.min_data)
print("join1")
a= join1.groupBy([join1.sector,join1.anno])\
      .agg(F.sum(join1.close))\
      .toDF("sector","anno","close_min")


join2= dataminmax.join(initDataFrame,["ticker","sector","anno"])\
      .where(initDataFrame.date==dataminmax.max_data)

print("join2")
b= join2.groupBy([join2.sector,join2.anno])\
      .agg(F.sum(join2.close))\
      .toDF("sector","anno","close_max")


join3=a.join(b,["sector","anno"])\
      .withColumn("variazione percentuale",(b.close_max-a.close_min)/a.close_min *100)\
      .drop(b.close_max).drop(a.close_min)\
      .toDF("sector","anno","variazione percentuale")

prova1= sommaclose.groupBy(["sector","anno"])\
      .agg(F.avg(sommaclose.sommaclose))\
      .toDF("sector","anno","sommaclose")

join4=volumeComplessivo.join(prova1,["sector","anno"])\
      .toDF("sector","anno","volumeComplessivo","mediaQuotazione")

finale=join3.join(join4,["sector","anno"])\
      .toDF("sector","anno","variazione percentuale","volumeComplessivo","mediaQuotazione")\
      .orderBy(["sector","anno"])
#final prints and save de output and the schema
#finale.show(100)
#time finish
endTimeQuery = time.clock()
runTimeQuery = endTimeQuery - startTimeQuery

print("time to output: ", runTimeQuery)
print("\n\n>>>>> END OF PROGRAM <<<<<\n\n")