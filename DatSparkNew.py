import json
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark import Row
from pyspark.sql import SQLContext
from pyspark.sql.functions import explode
from pyspark.sql.functions import expr
import pyspark.sql.functions as F
from pyspark.sql.functions import col
import os
import pandas
import findspark
from pyspark.sql.functions import mean as _mean, stddev as _stddev, col
import numpy as np
import math
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

spark_location='/Users/madisongipson/Library/Python/2.7/lib/python/site-packages/pyspark'
java8_location= '/Library/Java/JavaVirtualMachines/jdk1.8.0_251.jdk/Contents/Home'
os.environ['JAVA_HOME'] = java8_location
findspark.init(spark_home=spark_location)

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
year = "year"
months = "months"

#Handle command line args
thisCountry = "US"
startYear = 1900
endYear = 2019
startTrue = False
endTrue = False
foundCountry = False
if len(sys.argv) == 1:
    print("No country code specified, using US as the default.")
    print("No start and end years specified, using 1900-2019 as the time frame.")
if len(sys.argv) == 2: #.py COUNTRYCODE (assume 1900-2019)
    for countryCode in countriesDictionary:
        if sys.argv[1] == countryCode:
            thisCountry = sys.argv[1]
            foundCountry = True
            print("You indicated country code ", thisCountry)
            print("No start and end years specified, using 1900-2019 as the time frame.")
    if foundCountry == False:
        print("Invalid country code.")
        sys.exit(2)
if len(sys.argv) == 3: #.py COUNTRYCODE SINGLEYEAR
    #check if second arg is a country code
    for countryCode in countriesDictionary:
        if sys.argv[1] == countryCode:
            thisCountry = sys.argv[1]
            foundCountry = True
            print("You indicated country code ", thisCountry)
    if foundCountry == False:
        print("Invalid country code.")
        sys.exit(2)
    #check if third arg is a valid year
    if int(sys.argv[2]) > 1899 and int(sys.argv[2]) < 2020: #latest start year will be 2019
        startYear = int(sys.argv[2])
        endYear = startYear + 1
        print("You indicated start year ", startYear)
        print("No end year specified, using ", endYear, "as end year.")
        startTrue = True
    if startTrue == False:
        print("Invalid start year.")
        sys.exit(2)
if len(sys.argv) == 4: #.py COUNTRYCODE STARTYEAR ENDYEAR
    for countryCode in countriesDictionary:
        if sys.argv[1] == countryCode:
            thisCountry = sys.argv[1]
            foundCountry = True
            print("You indicated country code ", thisCountry)
    if foundCountry == False:
        print("Invalid country code.")
        sys.exit(2)
    if int(sys.argv[2]) > 1899 and int(sys.argv[2]) < 2020:
        startYear = int(sys.argv[2])
        print("You indicated start year ", startYear)
        startTrue = True
    if startTrue == False:
        print("Invalid start year.")
        sys.exit(2)
    if int(sys.argv[3]) > 1900 and int(sys.argv[3]) <= 2020:
        endYear = int(sys.argv[3])
        print("You indicated end year ", endYear)
        endTrue = True
    if endTrue == False:
        print("Invalid end year.")
        sys.exit(2)
    if startYear > endYear:
        print("Start year cannot be after end year.")
        sys.exit(2)
if len(sys.argv) > 4:
    print("Invalid number of arguments; usage is: program.py COUNTRYCODE STARTYEAR ENDYEAR.")
    sys.exit(2)

pathToJson = './TemperatureData.json'
dataFrame = spark.read.json(pathToJson, multiLine=False).repartition(4)
dataFrame = dataFrame.orderBy("year")
#dataFrame.printSchema()
df2 = dataFrame.withColumn("Avg", expr("""
    aggregate(
        flatten(months)
      , (double(0) as total, int(0) as cnt)
      , (x,y) -> (x.total+y, x.cnt+1)
      , z -> round(z.total/z.cnt,2)
    )
 """))

dataFrame.unpersist()

# = = = = =

def makeQuery(string, string2):
    query = """
        aggregate(
            {col}
        , (double(0) as total, int(0) as cnt)
        , (x,y) -> (x.total+y, x.cnt+1)
        , z -> round(z.total/z.cnt,2)
        ) AS `{name}`
    """.format(col=string, name=string2)
    return query

df2 = df2.selectExpr("*", makeQuery("months[0]", "JanAvg"))
df2 = df2.selectExpr("*", makeQuery("months[1]", "FebAvg"))
df2 = df2.selectExpr("*", makeQuery("months[2]", "MarAvg"))
df2 = df2.selectExpr("*", makeQuery("months[3]", "AprAvg"))
df2 = df2.selectExpr("*", makeQuery("months[4]", "MayAvg"))
df2 = df2.selectExpr("*", makeQuery("months[5]", "JunAvg"))
df2 = df2.selectExpr("*", makeQuery("months[6]", "JulAvg"))
df2 = df2.selectExpr("*", makeQuery("months[7]", "AugAvg"))
df2 = df2.selectExpr("*", makeQuery("months[8]", "SepAvg"))
df2 = df2.selectExpr("*", makeQuery("months[9]", "OctAvg"))
df2 = df2.selectExpr("*", makeQuery("months[10]", "NovAvg"))
df2 = df2.selectExpr("*", makeQuery("months[11]", "DecAvg"))

dfCountry = df2.filter(df2.country == thisCountry)

yearList = []
for i in range(startYear, endYear+1):
    yearList.append(i)
dfCountry = dfCountry.where(dfCountry.year.isin(yearList))
dfCountry.show()


monthsAvgStrArr = ["JanAvg", "FebAvg", "MarAvg", "AprAvg", "MayAvg", "JunAvg", "JulAvg", "AugAvg", "SepAvg", "OctAvg", "NovAvg", "DecAvg"]
monthsAvgChangeDict = {}
for m in monthsAvgStrArr:
    monthsAvgChangeDict[m] = []

def tryFloat(m):
    try:
        f = float(m)
        return f
    except:
        return None

def findAvgTempChange(arr, length):
    theSum = 0
    numNone = 0
    for element in arr:
        if element != None:
            theSum += element
        else:
            numNone += 1
    return (theSum/(length-numNone))

"""
monthValsDict = {}
dfCountryUS = dfCountryUS.toPandas()
print(dfCountryUS)
def fillArrs():
    for m in monthsAvgStrArr:
        tempDF = dfCountryUS[m].tolist()#dfCountryUS.select(m).collect()
        print(tempDF)
        #tempArr = [tryFloat(row[m]) for row in tempDF]
        monthValsDict[m] = tempDF

fillArrs()
"""
dfCountry = dfCountry.toPandas()
def calcAvgChangeForMonth(m):
    tempArr = dfCountry[m].tolist()
    #1900-2020
    #1950-2000
    year = 0
    while year != len(yearList)-1: #arr starts at 0, years start at 1900 and go to 2020/2019
        if tempArr[year] != None and tempArr[year+1] != None and math.isnan(tempArr[year]) == False and math.isnan(tempArr[year+1]) == False:
            changeInYears = tempArr[year+1] - tempArr[year]
            monthsAvgChangeDict[m].append(changeInYears)
        year += 1
    monthsAvgChangeDict[m] = findAvgTempChange(monthsAvgChangeDict[m], len(monthsAvgChangeDict[m]))
    #gives dictionary with average yearly temp change per month

for m in monthsAvgStrArr:
    calcAvgChangeForMonth(m)

#print(monthsAvgChangeDict)

tempArray = []
for k, v in monthsAvgChangeDict.items():
    tempArray.append(v)
totalAvgYearly = findAvgTempChange(tempArray, len(tempArray))
print("===== COUNTRY IS: " + thisCountry + " =====")
print("Total average yearly change in temperature from " + str(startYear) + " to " + str(endYear) + ": " + str(totalAvgYearly) + " degrees C")
print("Total average change in temperature from " + str(startYear) + " to " + str(endYear) + ": " + str(totalAvgYearly*(endYear-startYear)) + " degrees C")
print("===== ===== ===== ===== ==")


pd_df = dfCountry
#pd_df=dfCountry.toPandas()
ax=plt.gca()
pd_df.plot(kind='line',x='year',y='JanAvg',color='#000000',ax=ax) #black
pd_df.plot(kind='line',x='year',y='FebAvg',color='#FD25A5',ax=ax) #pink
pd_df.plot(kind='line',x='year',y='MarAvg',color='#33FFD6',ax=ax) #lightblue
pd_df.plot(kind='line',x='year',y='AprAvg',color='#52FF33',ax=ax) #brightgreen
pd_df.plot(kind='line',x='year',y='MayAvg',color='#FF8933',ax=ax) #orange
pd_df.plot(kind='line',x='year',y='JunAvg',color='#FF5733',ax=ax) #brightred
pd_df.plot(kind='line',x='year',y='JulAvg',color='#FFF533',ax=ax) #brightyellow
pd_df.plot(kind='line',x='year',y='AugAvg',color='#7908E5',ax=ax) #purple
pd_df.plot(kind='line',x='year',y='SepAvg',color='#087800',ax=ax) #darkgreen
pd_df.plot(kind='line',x='year',y='OctAvg',color='#F7C500',ax=ax) #mustard
pd_df.plot(kind='line',x='year',y='NovAvg',color='#B60000',ax=ax) #darkred
pd_df.plot(kind='line',x='year',y='DecAvg',color='#002Ec2',ax=ax) #darkblue
box=ax.get_position()
ax.set_position([box.x0, box.y0+(box.height*0.2), box.width, box.height*0.8])
ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), ncol=4)
plt.title("Climate Data", loc='center')
plt.xlabel("Year")
plt.ylabel("Temperature (Celcius)")
plt.savefig("climate_data.png")
plt.show()



