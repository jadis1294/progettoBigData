from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession,DataFrame
from pyspark.sql.functions import lit, concat, when, rand
from pyspark.sql import functions as F
import scipy.sparse as sparse
from operator import itemgetter
from pyspark.sql import SQLContext
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
    spark = SparkSession(sc)
    return (sc, sqlContext, spark)
sc, sqlContext, spark = init_spark("Homework1", "local[4]")

startTimeQuery = time.clock()

#Load the csv without the header and create de init DataFrame with the schema
doc1= spark.read.load( "/home/Jadis/Documenti/bigData/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv",\
       format="csv", sep=",",header=True) \
      .toDF( "ticker","open","close","adj","low","max","volume","date" )  

init=doc1.withColumn('anno', concat(doc1.date.substr(1, 4)))
#take the years>2004
doc= init.withColumn('anno',when(init.anno >= 1998, init.anno)).drop(init.anno)
initDataFrame= doc.filter(doc.anno.isNotNull())

#initDataFrame.orderBy(rand()).show(200)
#create variable
prezzoMin=initDataFrame.groupBy("ticker").agg(F.min(initDataFrame.low)).toDF("ticker","prezzoMin")
prezzoMax=initDataFrame.groupBy("ticker").agg(F.max(initDataFrame.max)).toDF("ticker","prezzoMax")
volume=initDataFrame.groupBy("ticker").agg(F.avg(initDataFrame.volume)).toDF("ticker","avg_daily_volume")


#join the different variable into a final dataframe
closeOpenDF = prezzoMax.join(prezzoMin,"ticker")
betterActionsDF = closeOpenDF.withColumn("crescita",((closeOpenDF.prezzoMax-closeOpenDF.prezzoMin)/closeOpenDF.prezzoMin)*100)
finalDf = betterActionsDF.join(volume,"ticker").orderBy("crescita",ascending=False)

#time finish
endTimeQuery = time.clock()
runTimeQuery = endTimeQuery - startTimeQuery
#final prints
#finalDf.show(300)
print("time to output: ", runTimeQuery)
print("\n\n>>>>> END OF PROGRAM <<<<<\n\n")