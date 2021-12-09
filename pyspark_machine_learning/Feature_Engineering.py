# library and working directory
from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
import multiprocessing
# import pyspark.sql.functions as F
from pyspark.sql.functions import col, when, count, udf
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

os.chdir("Documents/5202/new_as2")

'''******************************'''
'''

██████╗░░█████╗░██████╗░████████╗  ██╗
██╔══██╗██╔══██╗██╔══██╗╚══██╔══╝  ██║
██████╔╝███████║██████╔╝░░░██║░░░  ██║
██╔═══╝░██╔══██║██╔══██╗░░░██║░░░  ██║
██║░░░░░██║░░██║██║░░██║░░░██║░░░  ██║
╚═╝░░░░░╚═╝░░╚═╝╚═╝░░╚═╝░░░╚═╝░░░  ╚═╝

--------------Data Loading, Cleaning, Labelling, and Exploration--------------

'''
''' -------------------------- 1.1 Data loading -------------------------- '''
'''
1 pyspark enviroment
'''

master = "local[*]"
app_name = "Assignment 2"
spark_conf = SparkConf().setMaster(master).setAppName(
    app_name).set("spark.cores.max", multiprocessing.cpu_count()).set("spark.driver.memory", "20G")
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
sc = spark.sparkContext

'''
2 Read 20 files of "csv" and convert them to desired format
'''

# read file 20 files and make to 1

flightsRawDf = spark.read.format("csv").\
    load("flight-delays/flight1.csv", header=True, inferSchema=True)

for i in range(2, 21):
    file_name = "flight-delays/flight" + str(i) + ".csv"
    flightsRawDf_new = spark.read.format("csv").\
        load(file_name, header=True, inferSchema=True)
    flightsRawDf = flightsRawDf.union(flightsRawDf_new)

# Check format

flightsRawDf.printSchema()
'''
3 Obtain the list of columns from flightsRawDf and name this variable as
  allColumnFlights
'''

allColumnFlights = flightsRawDf.columns

''' -------------------------- 1.2 Data cleaning -------------------------- '''
'''
1. Check missing values and display the number of missing values for each column.
   Find the pattern.
'''

## Use pypark to find the missing value
flightsRawDf.count()
flightsRawDf.select([count(when(col(c).isNull(), c)).alias(c) for c in flightsRawDf.columns]).show(vertical=True)

# validation using pandas
df = flightsRawDf.toPandas()
df.isna().sum()

## Find the Pattern
# flightsRawDf.select(col("MONTH")).distinct().show()
allColumnFlights
flightsRawDf.filter(col("CANCELLATION_REASON").isNull()).select(col("CANCELLED")).distinct().show()
flightsRawDf.filter(col("CANCELLATION_REASON").isNull()).groupBy(col("CANCELLED")).count().show()

# Missing value analysis for delays
flightsRawDf.filter(col("AIR_SYSTEM_DELAY").isNull()).groupBy(col("AIRLINE_DELAY")).count().show()
flightsRawDf.filter(col("AIR_SYSTEM_DELAY").isNull()).groupBy(col("LATE_AIRCRAFT_DELAY")).count().show()
flightsRawDf.filter(col("AIR_SYSTEM_DELAY").isNull()).groupBy(col("WEATHER_DELAY")).count().show()

flightsRawDf.groupBy(col("CANCELLED")).count().show()
flightsRawDf.filter(col("AIR_SYSTEM_DELAY").isNull()).groupBy(col("CANCELLED")).count().show()

flightsRawDf.filter(col("AIR_SYSTEM_DELAY").isNull()).filter(col("CANCELLED")==0).select("ARRIVAL_DELAY").summary().show()
# Hence, all flights which has ARRIVAL_DELAY less than 15 will not be considered as delay here.
# Thus, the AIR_SYSTEM_DELAY and other 4 related columns will be null. And all CANCELLED flights are also having null Value
# for these five columns

'''
2. Remove columns and rows
'''
# a. create find_removed_columns

# try features

new_df = flightsRawDf.select([count(when(col(c).isNull(), c)).alias(c) for c in flightsRawDf.columns])
total_lenth = flightsRawDf.count()
new_df.select(flightsRawDf.columns[22]).collect()[0][0]


# function


def find_removed_columns(x, df):
    threshold = x/100
    new_df = df.select([count(when(col(c).isNull(), c)).alias(c) for c in flightsRawDf.columns])
    total_lenth = df.count()
    removedColumns = []
    for col_name in new_df.columns:
        if new_df.select(col_name).collect()[0][0]/total_lenth > threshold:
            removedColumns.append(col_name)
    return removedColumns


removedColumns = find_removed_columns(10, flightsRawDf)

# b. write function eliminate_columns

# try features

[x for x in flightsRawDf.columns if x not in removedColumns]

# functions


def eliminate_columns(removedColumns, df):
    survive_columns = [x for x in flightsRawDf.columns if x not in removedColumns]
    return flightsRawDf.select(survive_columns)


flightsRawDf = eliminate_columns(removedColumns, flightsRawDf)

len(allColumnFlights)
len(flightsRawDf.columns)

# c drop rows with null Value

# try

ll = flightsRawDf.na.drop("any")
ll.select([count(when(col(c).isNull(), c)).alias(c) for c in flightsRawDf.columns]).show(vertical=True)
ll.count()
# make the flightsDf

flightsDf = flightsRawDf.na.drop()
flightsDf.show()

# Display row numbers
print(("The flightsDf dataframe has {} columns and {} rows").format(len(flightsDf.columns),flightsDf.count()))


''' -------------------------- 1.3 Data Labelling -------------------------- '''
# 1
# a Make 2 columns binaryArrDelay and binaryDeptDelay

lst_delay_arrival = [x[0] for x in flightsDf.select(col("ARRIVAL_DELAY")).collect()]
lst_delay_departure = [x[0] for x in flightsDf.select(col("DEPARTURE_DELAY")).collect()]
lst_binary_delay_arrival = list(map(lambda x: 1 if x > 0 else 0, lst_delay_arrival))
lst_binary_delay_departure = list(map(lambda x: 1 if x > 0 else 0, lst_delay_departure))
df_1 = flightsDf.toPandas()
df_1["binaryArrDelay"] = lst_binary_delay_arrival
df_1["binaryDeptDelay"] = lst_binary_delay_departure
flightsDf_1 = spark.createDataFrame(df_1)
flightsDf = flightsDf_1
flightsDf.show()
len(flightsDf.columns)
flightsDf.groupBy(col("binaryArrDelay")).count().show()
flightsDf.groupBy(col("binaryDeptDelay")).count().show()

# Pyspark_quicker
flightsDf = flightsDf.withColumn("binaryArrDelay",when(col("ARRIVAL_DELAY")>0,1).otherwise(0))
flightsDf = flightsDf.withColumn("binaryDeptDelay",when(col("DEPARTURE_DELAY")>0,1).otherwise(0))

# b Create multiclass label multiClassArrDelay and multiCassDeptDelay

multiClassArrDelay = list(map(lambda x: 0 if x < 5 else (2 if x > 20 else 1), lst_delay_arrival))
multiCassDeptDelay = list(map(lambda x: 0 if x < 5 else (2 if x > 20 else 1), lst_delay_departure))

df_1 = flightsDf.toPandas()
df_1["multiClassArrDelay"] = multiClassArrDelay
df_1["multiCassDeptDelay"] = multiCassDeptDelay
flightsDf_1 = spark.createDataFrame(df_1)
flightsDf = flightsDf_1
flightsDf.groupBy(col("multiClassArrDelay")).count().show()
flightsDf.groupBy(col("multiCassDeptDelay")).count().show()

# Pypark_quicker
flightsDf = flightsDf.withColumn("multiClassArrDelay", when(col("ARRIVAL_DELAY")<5, 0).otherwise(when(col("ARRIVAL_DELAY")>20, 2).otherwise(1)))
flightsDf = flightsDf.withColumn("multiCassDeptDelay", when(col("DEPARTURE_DELAY")<5, 0).otherwise(when(col("DEPARTURE_DELAY")>20, 2).otherwise(1)))


## 2. create a function to execute it automatically

#
def autolabelling(early, late, flightsDf):
    lst_delay_arrival = [x[0] for x in flightsDf.select(col("ARRIVAL_DELAY")).collect()]
    lst_delay_departure = [x[0] for x in flightsDf.select(col("DEPARTURE_DELAY")).collect()]
    multiClassArrDelay = list(map(lambda x: 0 if x < early else (2 if x > late else 1), lst_delay_arrival))
    multiCassDeptDelay = list(map(lambda x: 0 if x < early else (2 if x > late else 1), lst_delay_departure))
    df_1 = flightsDf.toPandas()
    df_1["multiClassArrDelay"] = multiClassArrDelay
    df_1["multiCassDeptDelay"] = multiCassDeptDelay
    return spark.createDataFrame(df_1)

#
def autolabelling(early, late, flightsDf):
    flightsDf = flightsDf.withColumn("multiClassArrDelay_1",when(col("ARRIVAL_DELAY")<early,0).otherwise(when(col("ARRIVAL_DELAY")>late,2).otherwise(1)))
    flightsDf = flightsDf.withColumn("multiCassDeptDelay_1",when(col("DEPARTURE_DELAY")<early,0).otherwise(when(col("DEPARTURE_DELAY")>late,2).otherwise(1)))
    return flightsDf


autolabelling(-10, 10, flightsDf).show()

q25, q75 = np.percentile(np.array(lst_delay_arrival), [.25, .75])
bin_width = 2*(q75 - q25)*len(lst_delay_arrival)**(-1/3)
np.array(lst_delay_arrival).max()
np.array(lst_delay_arrival).min()
bins = round((np.array(lst_delay_arrival).max() - np.array(lst_delay_arrival).min())/bin_width)
for i in range(1):
    plt.hist(lst_delay_arrival, range=[-80, 80], density=True, bins=32, edgecolor='black')
    plt.xticks(np.arange(-80, 80, 5.0), rotation=70)
    plt.show()

q25, q75 = np.percentile(np.array(lst_delay_departure), [.25, .75])
bin_width = 2*(q75 - q25)*len(lst_delay_departure)**(-1/3)
np.array(lst_delay_departure).max()
np.array(lst_delay_departure).min()
bins = round((np.array(lst_delay_departure).max() - np.array(lst_delay_departure).min())/bin_width)
for i in range(1):
    plt.hist(lst_delay_departure, range=[-50, 50], density=True, bins=20, edgecolor='black')
    plt.xticks(np.arange(-50, 50, 5.0), rotation=70)
    plt.show()

# Less than 200 words comments (Make sense)


''' -------------------------- 1.4 Data Exploration / Exploratory Analysis -------------------------- '''

# Seperate categorical and numerical columns
flightsDf.printSchema()

categorical_columns = ["YEAR", "MONTH", "DAY", "DAY_OF_WEEK", "AIRLINE", "FLIGHT_NUMBER", "TAIL_NUMBER", "ORIGIN_AIRPORT",
                        "DESTINATION_AIRPORT", "DIVERTED", "CANCELLED", "binaryArrDelay", "binaryDeptDelay",
                        "multiClassArrDelay", "multiCassDeptDelay"]

numerical_columns = [x for x in flightsDf.columns if x not in categorical_columns]

## 1. Check numerical column

flightsDf.select(numerical_columns).summary().show()

## 2. Cetorical data, find distinct value
for cat_column in categorical_columns:
    flightsDf.groupBy(cat_column).count().show()

## 3. plot
# a. Percentage of flights that arrive late each month
from matplotlib.ticker import PercentFormatter

flightsDf.filter(col("multiClassArrDelay") == 2).groupBy("MONTH").count().orderBy("MONTH").show()
'''
MONTH	Total_flight	Total_late	Percentage
0	1	45900	7638	16.640523
1	2	40684	7620	18.729722
2	3	49580	7802	15.736184
3	4	48221	6462	13.400800
4	5	48977	7341	14.988668
5	6	49158	9616	19.561414
6	7	51415	8933	17.374307
7	8	49866	7616	15.272931
8	9	46459	4737	10.196087
9	10	48357	4667	9.651136
10	11	46203	5415	11.720018
11	12	46909	7845	16.723870
'''
lst_month = [x[1] for x in flightsDf.filter(col("multiClassArrDelay") == 2).groupBy("MONTH").count().orderBy("MONTH").collect()]
lst_month_all = [x[1] for x in flightsDf.groupBy("MONTH").count().orderBy("MONTH").collect()]
df_q1 = pd.DataFrame(columns=["MONTH","Total_flights","Late_flights","Percentage"])
df_q1["MONTH"] = range(1,13)
df_q1["Total_flights"] = lst_month_all
df_q1["Late_flights"] = lst_month
df_q1["Percentage"] = [lst_month[i]/lst_month_all[i] for i in range(12)]
df_q1
ax = df_q1.plot.bar(x='MONTH', y='Percentage', rot=0)
# b. Percentage of flights that arrive late each day of week

lst_day_of_week = [x[1] for x in flightsDf.filter(col("multiClassArrDelay") == 2).groupBy("DAY_OF_WEEK").count().orderBy("DAY_OF_WEEK").collect()]
lst_day_of_week_all = [x[1] for x in flightsDf.groupBy("DAY_OF_WEEK").count().orderBy("DAY_OF_WEEK").collect()]

df_q2 = pd.DataFrame(columns=["DAY_OF_WEEK","Total_flights","Late_flights","Percentage"])
df_q2["DAY_OF_WEEK"] = range(1,8)
df_q2["Total_flights"] = lst_day_of_week_all
df_q2["Late_flights"] = lst_day_of_week
df_q2["Percentage"] = [lst_day_of_week[i]/lst_day_of_week_all[i] for i in range(7)]
ax2 = df_q2.plot.bar(x='DAY_OF_WEEK', y='Percentage', rot=0)


# c. Percentage of delayed flights by airline
df_q3 = pd.DataFrame(columns=["AIRLINE","Total_flights","Late_flights","Percentage"])
lst_airline = [x[1] for x in flightsDf.filter(col("multiClassArrDelay") == 2).groupBy("AIRLINE").count().orderBy("AIRLINE").collect()]
lst_airline_all = [x[1] for x in flightsDf.groupBy("AIRLINE").count().orderBy("AIRLINE").collect()]
df_q3["AIRLINE"] = [x[0] for x in flightsDf.filter(col("multiClassArrDelay") == 2).groupBy("AIRLINE").count().orderBy("AIRLINE").collect()]
df_q3["Total_flights"] = lst_airline_all
df_q3["Late_flights"] = lst_airline
df_q3["Percentage"] = [lst_airline[i]/lst_airline_all[i] for i in range(len(lst_airline))]
ax3 = df_q3.plot.bar(x='AIRLINE', y='Percentage', rot=0)
'''******************************'''
