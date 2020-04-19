# Climate-Data-Analysis
Leveraged multi-core processing on a Raspberry Pi Cluster to process large datasets with Apache Spark and PySpark. Developed Python programs to convert file types and compress data for easier processing, then compute on the datasets to extract valuable statistics.

Starting with a .dat file of 100+ MB, DatToJson.py uses countries.txt to convert the .dat file into a compressed, easier to parse .json file.
DatSpark.py then reads the resulting .json file and uses Spark to handle the parallel processing that speeds up computations on the large dataset.

A performance analysis is shown in the PiProject.pdf, comparing speed, the number of threads utilized, and the number of partitions.
