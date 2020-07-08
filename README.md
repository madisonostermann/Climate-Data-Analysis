![Image of Dataframe](https://github.com/mgipson/Climate-Data-Analysis/blob/master/DataFrame.png) 

![Image of Graph](https://github.com/mgipson/Climate-Data-Analysis/blob/master/Graph.png)

# Climate Data Analysis Pt. 1
Leveraged multi-core processing on a Raspberry Pi Cluster to process large datasets with Apache Spark and PySpark. Developed Python programs to convert file types, compress data for easier processing, and compute on the datasets to extract valuable statistics.

Starting with a .dat file of 100+ MB (from https://www1.ncdc.noaa.gov/pub/data/ghcn/v4/), DatToJson.py uses countries.txt to convert the .dat file into a compressed, easier to parse .json file.
DatSpark.py then reads the resulting .json file and uses Spark to handle the parallel processing that speeds up computations on the large dataset.

A performance analysis is shown in the PiProject.pdf, comparing speed, the number of threads utilized, and the number of partitions.

# Climate Data Analysis Pt. 2
Added to DatSpark.py significantly, changes are in DatSparkNew.py. This new-and-improved program takes parameters such as the country you would like analytics for, as well as the year range (1900-2020). It averages station readings for each month into a "monthly average," which is then used to find the average temperature change for the year range given, and graphed using PyPlot.

The "PERFORMANCE ANALYSIS OF PROCESSING GHCN CLIMATE DATA USING CLUSTER COMPUTING AND APACHE SPARK" paper details the background research, implementation details, and performance analysis for this project.

To run, you'll need to download spark, java8, findspark, and pandas, and change the file paths specific to your machine.
*Java 8 was used because this program was intended to run on a Raspberry Pi cluster with Apache Spark, requiring Java 8.

