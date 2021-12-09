############################### library and working directory
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import pyspark
import os
import multiprocessing
from pyspark.sql.functions import *
os.chdir("5202/new_as1")

#******************************
'''

░██████╗░██╗░░░██╗███████╗░██████╗████████╗██╗░█████╗░███╗░░██╗  ░░███╗░░
██╔═══██╗██║░░░██║██╔════╝██╔════╝╚══██╔══╝██║██╔══██╗████╗░██║  ░████║░░
██║██╗██║██║░░░██║█████╗░░╚█████╗░░░░██║░░░██║██║░░██║██╔██╗██║  ██╔██║░░
╚██████╔╝██║░░░██║██╔══╝░░░╚═══██╗░░░██║░░░██║██║░░██║██║╚████║  ╚═╝██║░░
░╚═██╔═╝░╚██████╔╝███████╗██████╔╝░░░██║░░░██║╚█████╔╝██║░╚███║  ███████╗
░░░╚═╝░░░░╚═════╝░╚══════╝╚═════╝░░░░╚═╝░░░╚═╝░╚════╝░╚═╝░░╚══╝  ╚══════╝

'''
''' -------------------------- 1.1 Data preparation and loading -------------------------- '''

#### 1 pyspark enviroment

master = "local[*]"
app_name = "Assignment 1"
spark_conf = SparkConf().setMaster(master).setAppName(
    app_name).set("spark.cores.max", multiprocessing.cpu_count())
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
sc = spark.sparkContext

#### 2 Read 20 files of "csv" and convert them to desired format

# read file 20 files and make to 1

file_1 = sc.textFile('flight-delays/flight1.csv', 1)

header = file_1.first()

lst_column = header.split(",")

file = sc.emptyRDD()

for i in range(1,21):
    file_name = 'flight-delays/flight' + str(i)+'.csv'
    each_file = sc.textFile(file_name,1)
    new_file = each_file.filter(lambda l: not str(l).startswith(header))
    file = file.union(new_file)


# file_1.filter(lambda l: str(l).startswith(header)).union(file)


# split each line to multiple
file = file.map(lambda x: x.split(","))

# convert format

def convert_data(raw_data,element_index,convert_type):
    if convert_type == "float":
        try:
            raw_data[element_index] = float(raw_data[element_index])
        except:
            raw_data[element_index] = None
    elif convert_type == "int":
        try:
            raw_data[element_index] = int(raw_data[element_index])
        except:
            raw_data[element_index] = None
    else:
        raise ValueError('Should be "float" or "int" ')
    return raw_data


column_to_integer = ['YEAR', 'MONTH', 'DAY','DAY_OF_WEEK', 'FLIGHT_NUMBER']
column_to_float = ['DEPARTURE_DELAY','ARRIVAL_DELAY', 'ELAPSED_TIME', 'AIR_TIME', 'DISTANCE', 'TAXI_IN','TAXI_OUT']
column_to_integer_index = list(map(lambda x: lst_column.index(x),column_to_integer))
column_to_float_index = list(map(lambda x: lst_column.index(x), column_to_float))

# convert integer
file = file.map(lambda x : convert_data(x,0,"int"))
file = file.map(lambda x : convert_data(x,1,"int"))
file = file.map(lambda x : convert_data(x,2,"int"))
file = file.map(lambda x : convert_data(x,3,"int"))
file = file.map(lambda x : convert_data(x,5,"int"))
# convert float
column_to_float_index
file = file.map(lambda x : convert_data(x,11,"float"))
file = file.map(lambda x : convert_data(x,22,"float"))
file = file.map(lambda x : convert_data(x,15,"float"))
file = file.map(lambda x : convert_data(x,16,"float"))
file = file.map(lambda x : convert_data(x,17,"float"))
file = file.map(lambda x : convert_data(x,19,"float"))
file = file.map(lambda x : convert_data(x,12,"float"))


flights_rdd = file
flights_rdd.take(1)

# prepare airport data

airports_rdd = sc.textFile("flight-delays/airports.csv")
airports_rdd = airports_rdd.map(lambda x : x.split(","))
airports_rdd.collect()

#### 3

print(f"Number of lines for flights: {flights_rdd.count()}")
print(f"Number of lines for airports: {airports_rdd.count()}")
print(f"Number of partitions for flights: {flights_rdd.getNumPartitions()}")
print(f"Number of partitions for airports: {airports_rdd.getNumPartitions()}")
print(f"Number of columns for flights: {flights_rdd.map(lambda x: len(x)).first()}")
print(f"Number of columns for airports: {airports_rdd.map(lambda x: len(x)).first()}")



#******************************

''' -------------------------- 1.2 Dataset Partitioning in RDD -------------------------- '''
'''
The flgihts_rdd has 20 partitions, and airpots_rdd has 2 partitions
'''
#### 1

type(flights_rdd.map(lambda x : x[22]).first()) == float
def take_value(x):
    if type(x) == float:
        return x
    else:
        return 0

# find max

lst_column.index("ARRIVAL_DELAY")


flights_rdd.max(lambda x : take_value(x[22]))

# find min

flights_rdd.min(lambda x : take_value(x[22]))

# hash function
lst_column


def hash_function(key):
    ind = 0
    ind = key%12
    return ind

flights_rdd_partitioned = flights_rdd.map(lambda x : (x[1],x[0:])).partitionBy(12,hash_function)




# print records in each partition
def count_in_a_partition(idx, iterator):
  count = 0
  for _ in iterator:
    count += 1
  re_in_par = [idx,count]
  return re_in_par

re_in_par = flights_rdd_partitioned.mapPartitionsWithIndex(count_in_a_partition).collect()
re_in_par
dic = {}
for i in range(12):
    dic[i] = re_in_par[i*2-1]
dic



#******************************

''' -------------------------- 1.3 Query RDD -------------------------- '''


# flights per month

lst_column.index("MONTH")

set(flights_rdd.map(lambda x : x[1]).collect())

for i in range(1,13):
    print(f"Number of flights for month {i}: {flights_rdd.filter(lambda x: x[1]==i).count()}")

# display the average delay for each month
lst_column.index("MONTH")
lst_column.index("ARRIVAL_DELAY")
flights_rdd.map(lambda x: (x[22],1)).first()

flights_rdd.map(lambda x: x[22]).collect()

def reduce_add(x,y):
    if (type(x[0]) == float) & (type(y[0]) == float):
        return (x[0] + y[0], x[1] + y[1])
    else:
        try:
            return (x[0] + 0, x[1] + 0)
        except:
            return (0 + y[0] , 0 + y[1])


flights_rdd.filter(lambda x: x[1]==i).count()

flights_rdd.filter(lambda x: x[1]==i).map(lambda x: [x[22],1]).reduce(reduce_add)

'''for i in range(1,13):
    average_delay = flights_rdd.filter(lambda x: x[1]==i).map(lambda x: [x[22],1]).reduce(reduce_add)[0]/flights_rdd.filter(lambda x: x[1]==i).map(lambda x: [x[22],1]).reduce(reduce_add)[1]
    print(f"Average delay for month {i}: {average_delay}")
'''

type(flights_rdd.filter(lambda x: x[1]==i).filter(lambda x: x[22]!= None).map(lambda x: x[22]).reduce(lambda x,y: x+y))

for i in range(1,13):
    df_i = flights_rdd.filter(lambda x: x[1]==i).filter(lambda x: x[22]!= None).map(lambda x: x[22])
    print(f"Average delay for month {i}: {df_i.reduce(lambda x,y: x+y)/df_i.count()}")




#******************************


'''
░██████╗░██╗░░░██╗███████╗░██████╗████████╗██╗░█████╗░███╗░░██╗  ██████╗░
██╔═══██╗██║░░░██║██╔════╝██╔════╝╚══██╔══╝██║██╔══██╗████╗░██║  ╚════██╗
██║██╗██║██║░░░██║█████╗░░╚█████╗░░░░██║░░░██║██║░░██║██╔██╗██║  ░░███╔═╝
╚██████╔╝██║░░░██║██╔══╝░░░╚═══██╗░░░██║░░░██║██║░░██║██║╚████║  ██╔══╝░░
░╚═██╔═╝░╚██████╔╝███████╗██████╔╝░░░██║░░░██║╚█████╔╝██║░╚███║  ███████╗
░░░╚═╝░░░░╚═════╝░╚══════╝╚═════╝░░░░╚═╝░░░╚═╝░╚════╝░╚═╝░░╚══╝  ╚══════╝
'''


''' -------------------------- 2.1 Data Preparation and Loading -------------------------- '''

#1 load

flightsDf = spark.read.format("csv").load("flight-delays/flight1.csv",header=True,inferSchema=True)

for i in range(2,21):
    file_name = "flight-delays/flight" + str(i) + ".csv"
    flightsDf_new = spark.read.format("csv").load(file_name,header=True,inferSchema=True)
    flightsDf = flightsDf.union(flightsDf_new)

airportsDf = spark.read.format("csv").load("flight-delays/airports.csv",header=True,inferSchema=True)


# 2 printschema
airportsDf.printSchema()
flightsDf.printSchema()

''' -------------------------- 2.2 Query -------------------------- '''
# 1 Display all the flight events in January 2015 with five columns
flightsDf.columns
janFlightEventsAncAvgDf = flightsDf.filter((col('YEAR')==2015) & (col('MONTH')==1) & (col('ORIGIN_AIRPORT')=="ANC"))\
    .select(["Month","ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "DISTANCE", "ARRIVAL_DELAY"])

janFlightEventsAncAvgDf.show()

# 2 groupby and find

janFlightEventsAncAvgDf = janFlightEventsAncAvgDf.groupby(["ORIGIN_AIRPORT","DESTINATION_AIRPORT"])\
                            .agg(avg("ARRIVAL_DELAY").alias("AVERAGE_DELAY"))
janFlightEventsAncAvgDf = janFlightEventsAncAvgDf.sort(janFlightEventsAncAvgDf.AVERAGE_DELAY.desc())
janFlightEventsAncAvgDf.show()

# 3 Innerjoin

joinedSqlDf = airportsDf.join(janFlightEventsAncAvgDf,airportsDf.IATA_CODE==janFlightEventsAncAvgDf.ORIGIN_AIRPORT,how='inner')

joinedSqlDf.show()

''' -------------------------- 2.3 Analysis -------------------------- '''
flightsDf.createOrReplaceTempView('flights_sql')

# groupby and sorted
flightsDf.columns
flightsDf_by_day = flightsDf.groupby("DAY_OF_WEEK").agg(avg("ARRIVAL_DELAY").alias("MeanArrivalDelay"),sum("ARRIVAL_DELAY").alias("TotalTimeDelay")\
                    ,count("ARRIVAL_DELAY").alias("NumOfFlights"))

flightsDf_by_day = flightsDf_by_day.orderBy(flightsDf_by_day.MeanArrivalDelay.desc())



sql_flights_by_day = spark.sql('''
    SELECT DAY_OF_WEEK, avg(ARRIVAL_DELAY) as MeanArrivalDelay, sum(ARRIVAL_DELAY) as TotalTimeDelay, count(*) as NumOfFlights
    FROM flights_sql
    GROUP BY DAY_OF_WEEK
    ORDER BY MeanArrivalDelay DESC
''')
flightsDf_by_day.show()
sql_flights_by_day.show()

# 2



flightsDf_by_month = flightsDf.groupby("MONTH").agg(avg("ARRIVAL_DELAY").alias("MeanArrivalDelay"),sum("ARRIVAL_DELAY").alias("TotalTimeDelay")\
                    ,count("MONTH").alias("NumOfFlights"))


flightsDf_by_month = flightsDf_by_month.orderBy(flightsDf_by_month.MeanArrivalDelay)
flightsDf_by_month.show()

sql_flights_by_month = spark.sql('''
    SELECT MONTH, avg(ARRIVAL_DELAY) as MeanArrivalDelay, sum("ARRIVAL_DELAY") as TotalTimeDelay, count(*) as NumOfFlights
    FROM flights_sql
    GROUP BY MONTH
    ORDER BY MeanArrivalDelay
''')

sql_flights_by_month.show()

# 3

flightsDf_by_month_delay = flightsDf.groupby("MONTH").agg(avg("ARRIVAL_DELAY").alias("MeanArrivalDelay"),avg("DEPARTURE_DELAY")\
                    .alias("MeanDeptDelay"))
flightsDf_by_month_delay = flightsDf_by_month_delay.sort(flightsDf_by_month_delay.MeanDeptDelay.desc())


sql_flights_by_month_delay = spark.sql('''
    SELECT MONTH, avg(ARRIVAL_DELAY) as MeanArrivalDelay, avg(DEPARTURE_DELAY) as MeanDeptDelay
    FROM flights_sql
    GROUP BY MONTH
    ORDER BY MeanDeptDelay DESC
''')

flightsDf_by_month_delay.show()

sql_flights_by_month_delay.show()


#******************************

'''
░██████╗░██╗░░░██╗███████╗░██████╗████████╗██╗░█████╗░███╗░░██╗  ██████╗░
██╔═══██╗██║░░░██║██╔════╝██╔════╝╚══██╔══╝██║██╔══██╗████╗░██║  ╚════██╗
██║██╗██║██║░░░██║█████╗░░╚█████╗░░░░██║░░░██║██║░░██║██╔██╗██║  ░█████╔╝
╚██████╔╝██║░░░██║██╔══╝░░░╚═══██╗░░░██║░░░██║██║░░██║██║╚████║  ░╚═══██╗
░╚═██╔═╝░╚██████╔╝███████╗██████╔╝░░░██║░░░██║╚█████╔╝██║░╚███║  ██████╔╝
░░░╚═╝░░░░╚═════╝░╚══════╝╚═════╝░░░░╚═╝░░░╚═╝░╚════╝░╚═╝░░╚══╝  ╚═════╝░

Find the MONTH and DAY_OF_WEEK, number of flights, average departure delay,and average arrival delay, where TAIL_NUMBER = ‘N407AS’.
Note number of flights, average departure delay, and average arrival delay should be aggregatedseparately.
The query should be grouped by MONTH, DAY_OF_WEEK, and TAIL_NUMBER.

'''

# RDD
def reduce_add_q3(x,y):
    if y[1]!= None:
        return [x[0] + y[0], x[1] + y[1],x[2]+y[2]]
    else:
        return  [x[0] + 0, x[1] + 0,x[2]+0]


columns_need = ['MONTH','DAY_OF_WEEK', 'ARRIVAL_DELAY', 'TAIL_NUMBER',"DEasPARTURE_DELAY"]
column_need_index = list(map(lambda x: lst_column.index(x),columns_need))

%%time
q3_rdd = flights_rdd.filter(lambda x : x[6]=="N407AS").map(lambda x: ((x[3],x[1]),[1, x[22],x[11]]))
q3_rdd = q3_rdd.reduceByKey(reduce_add_q3).map(lambda x: [x[0][0],x[0][1],x[1][2]/x[1][0],x[1][1]/x[1][0]])
q3_rdd.collect()


# Data Frame
%%time
q3_df = flightsDf.filter(col("TAIL_NUMBER")=="N407AS").groupBy("DAY_OF_WEEK","MONTH","TAIL_NUMBER").agg(avg("DEPARTURE_DELAY")\
            .alias("AVG_DE_DELAY"),avg("ARRIVAL_DELAY").alias("AVG_AR_DELAY")).sort(["DAY_OF_WEEK","MONTH"])
q3_df.show()

# sql

%%time
sql_q3 = spark.sql('''
    SELECT DAY_OF_WEEK, MONTH, TAIL_NUMBER, avg(DEPARTURE_DELAY) as AVG_DE_DELAY, avg(ARRIVAL_DELAY) as AVG_AR_DELAY
    FROM flights_sql
    WHERE TAIL_NUMBER = "N407AS"
    GROUP BY DAY_OF_WEEK, MONTH, TAIL_NUMBER
''')

sql_q3.show()
#******************************













































































###########################
