import json
import pyspark
from pyspark.sql import SparkSession
from pyspark import Row
from pyspark.sql import SQLContext
from pyspark.sql.functions import explode
from pyspark.sql.functions import expr

sc = pyspark.SparkContext()
spark = SparkSession.builder.getOrCreate()

countriesDictionary = {}

with open('./countries.txt') as f: #make dictionary associating country codes with country names
    for line in f:
        countryCode = line[0] + line[1]
        if line[-2] == ' ':
            countryName = line[3:-2]
        else:
            countryName = line[3:-1]
        countriesDictionary[countryCode] = countryName

#rdd = sc.parallelize()

year = "year"



pathToJson = './TemperatureData.json'
dataFrame = spark.read.json(pathToJson, multiLine=False).repartition(1)
dataFrame = dataFrame.orderBy("year")

#fullTable.saveAsTextFile(fullTxt) #only on pi with spark
#fullTable.write.csv('fullTable.csv') #only on pi, spark 2.0+
#fullTable.write.format('com.intelli.spark.csv').save('fullTable.csv') #spark 1.4+
#fullTable.select("country", "year", "Avg").write.save("fullTable.parquet")

table = dataFrame.withColumn("Avg", expr("""
   aggregate(
       flatten(months)
     , (double(0) as total, int(0) as cnt)
     , (x,y) -> (x.total+y, x.cnt+1)
     , z -> round(z.total/z.cnt,2)
   )
"""))
table.filter(table.Avg.isNotNull()).show()

