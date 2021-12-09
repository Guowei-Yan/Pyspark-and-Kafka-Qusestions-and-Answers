# Import SparkConf class into program
from pyspark import SparkConf
import os
import pandas as pd
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import to_timestamp
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql.types import DataType, ArrayType, StructType,StructField, IntegerType, DateType, StringType, DoubleType, LongType
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
from time import sleep

os.chdir("Documents/5202/as2b/FIT5202as2partB")
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 pyspark-shell'

master = "local[2]"
app_name = "Task 3"
spark_conf = SparkConf().setMaster(master).setAppName(app_name).set("spark.cores.max", 2)

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.config(conf=spark_conf).config("spark.sql.session.timeZone", "UTC").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('ERROR')



topic = "flightTopic"
df_flight = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", topic) \
    .load()

df_flight.printSchema()


# 3
data_peek = pd.read_csv("flight-delays/flight1.csv")

schema_process_root = ArrayType(StructType([
    StructField('1', StringType(), True),
    StructField('2', StringType(), True),
    StructField('3', StringType(), True),
    StructField('4', StringType(), True),
    StructField('5', StringType(), True),
    StructField('6', StringType(), True),
    StructField('7', StringType(), True),
    ]))

schema_flight = ArrayType(StructType([
    StructField("AIRLINE",StringType(),True),
    StructField("AIRLINE_DELAY",StringType(),True),
    StructField("AIR_SYSTEM_DELAY",StringType(),True),
    StructField("AIR_TIME",StringType(),True),
    StructField("ARRIVAL_DELAY",StringType(),True),
    StructField("ARRIVAL_TIME",StringType(),True),
    StructField("CANCELLATION_REASON",StringType(),True),
    StructField("CANCELLED",StringType(),True),
    StructField("DAY",StringType(),True),
    StructField("DAY_OF_WEEK",StringType(),True),
    StructField("DEPARTURE_DELAY",StringType(),True),
    StructField("DEPARTURE_TIME",StringType(),True),
    StructField("DESTINATION_AIRPORT",StringType(),True),
    StructField("DISTANCE",StringType(),True),
    StructField("DIVERTED",StringType(),True),
    StructField("ELAPSED_TIME",StringType(),True),
    StructField("FLIGHT_NUMBER",StringType(),True),
    StructField("LATE_AIRCRAFT_DELAY",StringType(),True),
    StructField("MONTH",StringType(),True),
    StructField("ORIGIN_AIRPORT",StringType(),True),
    StructField("SCHEDULED_ARRIVAL",StringType(),True),
    StructField("SCHEDULED_DEPARTURE",StringType(),True),
    StructField("SCHEDULED_TIME",StringType(),True),
    StructField("SECURITY_DELAY",StringType(),True),
    StructField("TAIL_NUMBER",StringType(),True),
    StructField("TAXI_IN",StringType(),True),
    StructField("TAXI_OUT",StringType(),True),
    StructField("WEATHER_DELAY",StringType(),True),
    StructField("WHEELS_OFF",StringType(),True),
    StructField("WHEELS_ON",StringType(),True),
    StructField("YEAR",StringType(),True),
    StructField("ts",LongType(),True)
    ]))




lst_column = [x["name"] for x in schema_flight.jsonValue()["elementType"]["fields"]]

for col_1 in data_peek.columns:
    if col_1 not in lst_column:
        print(col_1)

df_flight_1 = df_flight.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df_flight_1.printSchema()

df_flight_1 = df_flight_1.select(F.from_json(F.col("value").cast("string"), schema_process_root).alias("parsed_value"))

df_flight_1 = df_flight_1.select(F.explode(F.col("parsed_value")).alias('unnested_value'))

df_flight_1 = df_flight_1.select(
                    F.col("unnested_value.1").alias("1"),
                    F.col("unnested_value.2").alias("2"),
                    F.col("unnested_value.3").alias("3"),
                    F.col("unnested_value.4").alias("4"),
                    F.col("unnested_value.5").alias("5"),
                    F.col("unnested_value.6").alias("6"),
                    F.col("unnested_value.7").alias("7")
                )


df_flight_1.printSchema()

df_flight_full = df_flight_1.select(F.from_json(F.col("1").cast("string"), schema_flight).alias('parsed_value'))

df_flight_full = df_flight_full.select(F.explode(F.col("parsed_value")).alias('unnested_value'))

df_flight_full.printSchema()

df_flight_full_formatted = df_flight_full.select(
                    F.col("unnested_value.YEAR").cast("integer").alias("YEAR"),
                    F.col("unnested_value.MONTH").cast("integer").alias("MONTH"),
                    F.col("unnested_value.DAY").cast("integer").alias("DAY"),
                    F.col("unnested_value.DAY_OF_WEEK").cast("integer").alias("DAY_OF_WEEK"),
                    F.col("unnested_value.AIRLINE").alias("AIRLINE"),
                    F.col("unnested_value.FLIGHT_NUMBER").cast("integer").alias("FLIGHT_NUMBER"),
                    F.col("unnested_value.TAIL_NUMBER").alias("TAIL_NUMBER"),
                    F.col("unnested_value.ORIGIN_AIRPORT").alias("ORIGIN_AIRPORT"),
                    F.col("unnested_value.DESTINATION_AIRPORT").alias("DESTINATION_AIRPORT"),
                    F.col("unnested_value.SCHEDULED_DEPARTURE").cast("integer").alias("SCHEDULED_DEPARTURE"),
                    F.col("unnested_value.DEPARTURE_TIME").cast("integer").alias("DEPARTURE_TIME"),
                    F.col("unnested_value.DEPARTURE_DELAY").cast("integer").alias("DEPARTURE_DELAY"),
                    F.col("unnested_value.TAXI_OUT").cast("integer").alias("TAXI_OUT"),
                    F.col("unnested_value.WHEELS_OFF").cast("integer").alias("WHEELS_OFF"),
                    F.col("unnested_value.SCHEDULED_TIME").cast("integer").alias("SCHEDULED_TIME"),
                    F.col("unnested_value.ELAPSED_TIME").cast("integer").alias("ELAPSED_TIME"),
                    F.col("unnested_value.AIR_TIME").cast("integer").alias("AIR_TIME"),
                    F.col("unnested_value.DISTANCE").cast("integer").alias("DISTANCE"),
                    F.col("unnested_value.WHEELS_ON").cast("integer").alias("WHEELS_ON"),
                    F.col("unnested_value.TAXI_IN").cast("integer").alias("TAXI_IN"),
                    F.col("unnested_value.SCHEDULED_ARRIVAL").cast("integer").alias("SCHEDULED_ARRIVAL"),
                    F.col("unnested_value.ARRIVAL_TIME").cast("integer").alias("ARRIVAL_TIME"),
                    F.col("unnested_value.ARRIVAL_DELAY").cast("integer").alias("ARRIVAL_DELAY"),
                    F.col("unnested_value.DIVERTED").cast("integer").alias("DIVERTED"),
                    F.col("unnested_value.CANCELLED").cast("integer").alias("CANCELLED"),
                    F.col("unnested_value.CANCELLATION_REASON").alias("CANCELLATION_REASON"),
                    F.col("unnested_value.AIR_SYSTEM_DELAY").cast("integer").alias("AIR_SYSTEM_DELAY"),
                    F.col("unnested_value.SECURITY_DELAY").cast("integer").alias("SECURITY_DELAY"),
                    F.col("unnested_value.AIRLINE_DELAY").cast("integer").alias("AIRLINE_DELAY"),
                    F.col("unnested_value.LATE_AIRCRAFT_DELAY").cast("integer").alias("LATE_AIRCRAFT_DELAY"),
                    F.col("unnested_value.WEATHER_DELAY").cast("integer").alias("WEATHER_DELAY"),
                    F.col("unnested_value.ts").cast("integer").alias("ts")
                )
df_flight_full_formatted.printSchema()

for i in range(2,8):
    df_flight_each = df_flight_1.select(F.from_json(F.col(str(i)).cast("string"), schema_flight).alias('parsed_value'))
    df_flight_each = df_flight_each.select(F.explode(F.col("parsed_value")).alias('unnested_value'))
    df_flight_each_formatted = df_flight_each.select(
                        F.col("unnested_value.YEAR").cast("integer").alias("YEAR"),
                        F.col("unnested_value.MONTH").cast("integer").alias("MONTH"),
                        F.col("unnested_value.DAY").cast("integer").alias("DAY"),
                        F.col("unnested_value.DAY_OF_WEEK").cast("integer").alias("DAY_OF_WEEK"),
                        F.col("unnested_value.AIRLINE").alias("AIRLINE"),
                        F.col("unnested_value.FLIGHT_NUMBER").cast("integer").alias("FLIGHT_NUMBER"),
                        F.col("unnested_value.TAIL_NUMBER").alias("TAIL_NUMBER"),
                        F.col("unnested_value.ORIGIN_AIRPORT").alias("ORIGIN_AIRPORT"),
                        F.col("unnested_value.DESTINATION_AIRPORT").alias("DESTINATION_AIRPORT"),
                        F.col("unnested_value.SCHEDULED_DEPARTURE").cast("integer").alias("SCHEDULED_DEPARTURE"),
                        F.col("unnested_value.DEPARTURE_TIME").cast("integer").alias("DEPARTURE_TIME"),
                        F.col("unnested_value.DEPARTURE_DELAY").cast("integer").alias("DEPARTURE_DELAY"),
                        F.col("unnested_value.TAXI_OUT").cast("integer").alias("TAXI_OUT"),
                        F.col("unnested_value.WHEELS_OFF").cast("integer").alias("WHEELS_OFF"),
                        F.col("unnested_value.SCHEDULED_TIME").cast("integer").alias("SCHEDULED_TIME"),
                        F.col("unnested_value.ELAPSED_TIME").cast("integer").alias("ELAPSED_TIME"),
                        F.col("unnested_value.AIR_TIME").cast("integer").alias("AIR_TIME"),
                        F.col("unnested_value.DISTANCE").cast("integer").alias("DISTANCE"),
                        F.col("unnested_value.WHEELS_ON").cast("integer").alias("WHEELS_ON"),
                        F.col("unnested_value.TAXI_IN").cast("integer").alias("TAXI_IN"),
                        F.col("unnested_value.SCHEDULED_ARRIVAL").cast("integer").alias("SCHEDULED_ARRIVAL"),
                        F.col("unnested_value.ARRIVAL_TIME").cast("integer").alias("ARRIVAL_TIME"),
                        F.col("unnested_value.ARRIVAL_DELAY").cast("integer").alias("ARRIVAL_DELAY"),
                        F.col("unnested_value.DIVERTED").cast("integer").alias("DIVERTED"),
                        F.col("unnested_value.CANCELLED").cast("integer").alias("CANCELLED"),
                        F.col("unnested_value.CANCELLATION_REASON").alias("CANCELLATION_REASON"),
                        F.col("unnested_value.AIR_SYSTEM_DELAY").cast("integer").alias("AIR_SYSTEM_DELAY"),
                        F.col("unnested_value.SECURITY_DELAY").cast("integer").alias("SECURITY_DELAY"),
                        F.col("unnested_value.AIRLINE_DELAY").cast("integer").alias("AIRLINE_DELAY"),
                        F.col("unnested_value.LATE_AIRCRAFT_DELAY").cast("integer").alias("LATE_AIRCRAFT_DELAY"),
                        F.col("unnested_value.WEATHER_DELAY").cast("integer").alias("WEATHER_DELAY"),
                        F.col("unnested_value.ts").cast("integer").alias("ts")
                    )
    df_flight_full_formatted = df_flight_full_formatted.unionAll(df_flight_each_formatted)

df_flight_full_formatted.printSchema()

# 4

query_flights = df_flight_full_formatted.writeStream.format("parquet")\
        .outputMode("append")\
        .option("path", r"./flights.parquet")\
        .option("checkpointLocation", "flights.parquet/checkpoint")\
        .trigger(processingTime='5 seconds')\
        .start()

query_flights.stop()

query1 = df_flight_full_formatted.writeStream.queryName("counting").format("memory").outputMode("append").start()
query1.stop()
spark.sql("SELECT * FROM counting").show()

# 5

## Create pipeline

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml.classification import DecisionTreeClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

pickleRdd = sc.pickleFile("flightsDf").collect()

flightsDf = spark.createDataFrame(pickleRdd)

del pickleRdd

flightsDf = flightsDf.select(
                    F.col("YEAR").cast("integer").alias("YEAR"),
                    F.col("MONTH").cast("integer").alias("MONTH"),
                    F.col("DAY").cast("integer").alias("DAY"),
                    F.col("DAY_OF_WEEK").cast("integer").alias("DAY_OF_WEEK"),
                    F.col("AIRLINE").alias("AIRLINE"),
                    F.col("FLIGHT_NUMBER").cast("integer").alias("FLIGHT_NUMBER"),
                    F.col("TAIL_NUMBER").alias("TAIL_NUMBER"),
                    F.col("ORIGIN_AIRPORT").alias("ORIGIN_AIRPORT"),
                    F.col("DESTINATION_AIRPORT").alias("DESTINATION_AIRPORT"),
                    F.col("SCHEDULED_DEPARTURE").cast("integer").alias("SCHEDULED_DEPARTURE"),
                    F.col("DEPARTURE_TIME").cast("integer").alias("DEPARTURE_TIME"),
                    F.col("DEPARTURE_DELAY").cast("integer").alias("DEPARTURE_DELAY"),
                    F.col("TAXI_OUT").cast("integer").alias("TAXI_OUT"),
                    F.col("WHEELS_OFF").cast("integer").alias("WHEELS_OFF"),
                    F.col("SCHEDULED_TIME").cast("integer").alias("SCHEDULED_TIME"),
                    F.col("ELAPSED_TIME").cast("integer").alias("ELAPSED_TIME"),
                    F.col("AIR_TIME").cast("integer").alias("AIR_TIME"),
                    F.col("DISTANCE").cast("integer").alias("DISTANCE"),
                    F.col("WHEELS_ON").cast("integer").alias("WHEELS_ON"),
                    F.col("TAXI_IN").cast("integer").alias("TAXI_IN"),
                    F.col("SCHEDULED_ARRIVAL").cast("integer").alias("SCHEDULED_ARRIVAL"),
                    F.col("ARRIVAL_TIME").cast("integer").alias("ARRIVAL_TIME"),
                    F.col("ARRIVAL_DELAY").cast("integer").alias("ARRIVAL_DELAY"),
                    F.col("DIVERTED").cast("integer").alias("DIVERTED"),
                    F.col("CANCELLED").cast("integer").alias("CANCELLED")
                )


flightsDf = flightsDf.withColumn("DELAYED",F.when(F.col("ARRIVAL_DELAY")>0,1).otherwise(0))


flightsDf.printSchema()
cc.printSchema()
categorical_columns = ["YEAR", "MONTH", "DAY", "DAY_OF_WEEK", "AIRLINE", "FLIGHT_NUMBER", "TAIL_NUMBER", "ORIGIN_AIRPORT",
                        "DESTINATION_AIRPORT", "DIVERTED", "CANCELLED", "DELAYED"]

feature_columns = ["MONTH","DAY","DAY_OF_WEEK","AIRLINE","ORIGIN_AIRPORT","DESTINATION_AIRPORT",
                    "DEPARTURE_TIME","TAXI_OUT","WHEELS_OFF","ELAPSED_TIME","TAXI_IN","SCHEDULED_ARRIVAL",
                    "ARRIVAL_TIME"]

categorical_features = [x for x in categorical_columns if x != "DELAYED"]

lst_indexer = [x+"_index" for x in feature_columns if x in categorical_features]
indexer = StringIndexer(inputCols=[x for x in feature_columns if x in categorical_features], outputCols = lst_indexer)
lst_encoder = [x+"_oneHot" for x in feature_columns if x in categorical_features]
encoder = OneHotEncoder(inputCols=lst_indexer, outputCols=lst_encoder)
lst_numeric_cols = [x for x in feature_columns if x not in categorical_features]
all_features_cols = lst_encoder + lst_numeric_cols
assembler = VectorAssembler(inputCols = all_features_cols, outputCol = "features")
label_indexer = StringIndexer(inputCols = ["DELAYED"], outputCols = ["DELAYED_INDEX"])
gbt_arr = GBTClassifier(labelCol="DELAYED_INDEX", featuresCol="features", maxIter=10)
lst_pipeline = [indexer, encoder, assembler, label_indexer, gbt_arr]
pipeline = Pipeline(stages = lst_pipeline)

pipelineModel = pipeline.fit(flightsDf)
df_ml = pipelineModel.transform(flightsDf)
pipelineModel.write().overwrite().save("pipeline_model")

bb = BinaryClassificationEvaluator(labelCol = "DELAYED_INDEX", rawPredictionCol="rawPrediction")
auc_dt = bb.evaluate(df_ml)




df_ml.show()

## Read Pipeline

mPath =  "pipeline_model"

from pyspark.ml.pipeline import PipelineModel
pipelineModel = PipelineModel.load(mPath)



df_flight_full_model = df_flight_full_formatted.na.drop(subset=["ARRIVAL_DELAY"]).withColumn("DELAYED",F.when(F.col("ARRIVAL_DELAY")>0,1).otherwise(0))


df_flight_prediction = pipelineModel.transform(df_flight_full_model)


query_flight_count = df_flight_prediction.writeStream.queryName("counting").format("memory").outputMode("append").start()
query_flight_count.stop()

spark.sql("SELECT * FROM counting").show()

#df_test = spark.sql("SELECT * FROM counting")

#df_test_transform = pipelineModel.transform(df_test)

#df_test_transform.show()

cc = df_flight_prediction.withColumn("event_time",F.col('ts').cast('timestamp'))
cc = cc.withColumn("CORRECT", F.when(F.col("DELAYED_INDEX")==F.col("prediction"),1).otherwise(0))

# 6
import sys
import warnings
import datetime
import time
import matplotlib.pyplot as plt

#query_flight_count = cc.writeStream.queryName("counting").format("memory").outputMode("append").start()

int_time = lambda x: int(x.strftime("%M"))
round_time = lambda x: x.replace(minute = int_time(x)+1, second= 0, microsecond=0) if int_time(x) %2!=0 else x.replace(minute = int_time(x)+2, second= 0, microsecond=0)
df_flight_count = cc \
    .filter((F.col("DAY_OF_WEEK").isin([1,2,3])) & (F.col("event_time") >= round_time(datetime.datetime.now(tz=datetime.timezone.utc))))\
    .withWatermark("event_time", "6 minutes")\
    .groupBy(F.window(cc.event_time, "120 seconds"), cc['DAY_OF_WEEK'].alias("KEY"))\
    .agg(F.sum(F.col("CORRECT")).alias("RIGHT"),F.count(F.col("DAY")).alias("COUNT"))\
    .select("KEY", "window", (F.col("RIGHT")/F.col("COUNT")).alias("ACCURACY"),"COUNT")\
    .sort("window")


query_flight_count = df_flight_count.writeStream.queryName("counting").format("memory").outputMode("complete").start()
now_time = datetime.datetime.now(tz=datetime.timezone.utc)
target_time = round_time(now_time) + datetime.timedelta(minutes=6)

sys.stdout.write('Collecting Data: \n')
while True:
    if datetime.datetime.now(tz=datetime.timezone.utc) <= target_time:
        sys.stdout.write('\r')
        i = round((datetime.datetime.now(tz=datetime.timezone.utc)-now_time).total_seconds()/(target_time-now_time).total_seconds(),3)*100
        # the exact output you're looking for:
        sys.stdout.write("[%-40s] %d%%" % ('='*round(i/100*40), i))
        sys.stdout.flush()
    else:
        query_flight_count.stop()
        query_flight_count.stop()
        df_all = spark.sql("select * from counting order by window").toPandas()
        x_all = list(set(x['start'] for x in df_all['window'].to_list()))
        x_all.sort()
        x_all = [x.strftime("%H:%M:%S") for x in x_all]
        y_1_1 = df_all[df_all["KEY"]==1]["COUNT"].to_list()
        y_1_2 = df_all[df_all["KEY"]==2]["COUNT"].to_list()
        y_1_3 = df_all[df_all["KEY"]==3]["COUNT"].to_list()
        y_2_1 = df_all[df_all["KEY"]==1]["ACCURACY"].to_list()
        y_2_2 = df_all[df_all["KEY"]==2]["ACCURACY"].to_list()
        y_2_3 = df_all[df_all["KEY"]==3]["ACCURACY"].to_list()
        width = 19
        height = 8
        fig = plt.figure(figsize=(width,height)) # create new figure
        fig.subplots_adjust(hspace=0.8)
        ax1 = fig.add_subplot(121) # adding the subplot axes to the given grid position
        ax1.set_xlabel('Time')
        ax1.set_ylabel('Count')
        ax1.title.set_text('Time Vs Count')
        ax2 = fig.add_subplot(122) # adding the subplot axes to the given grid position
        ax2.set_xlabel('Time')
        ax2.set_ylabel('Accuracy')
        ax2.title.set_text('Time Vs Accuracy')
        fig.suptitle('Real-time uniform stream data visualization') # giving figure a title
        fig.show() # displaying the figure
        fig.canvas.draw() # drawing on the canvas
        ax1.clear()
        ax1.plot(x_all, y_1_1, '-b',label="KEY == 1")
        ax1.plot(x_all, y_1_2, '-y',label="KEY == 2")
        ax1.plot(x_all, y_1_3, '-r',label="KEY == 3")
        ax1.set_xlabel('Time')
        ax1.set_ylabel('Value')
        ax2.clear()
        ax2.plot(x_all, y_2_1, '-b',label="KEY == 1")
        ax2.plot(x_all, y_2_2, '-y',label="KEY == 2")
        ax2.plot(x_all, y_2_3, '-r',label="KEY == 3")
        ax1.set_xticks(x_all)
        ax1.set_xlabel('Time')
        ax1.set_ylabel('Value')
        leg = ax1.legend()
        fig.canvas.draw()
        fig.show()
        break













# old version Do not use

now_time = datetime.datetime.now()
a = True
while True:
    df_all = spark.sql("select * from counting order by window").toPandas()
    # Get starting timestamp to plot both graphs
    df_all["end_time"] = list(map(lambda x:  x["end"], df_all["window"]))
    df_all = df_all[df_all["end_time"] >= now_time-datetime.timedelta(minutes=2)]
    if len(df_all)>=9:
        df_all = df_all.reset_index().iloc[:,1:]
        start_time = df_all['window'][len(df_all)-1]["start"]
        end_time = df_all['window'][len(df_all)-1]["end"]
        end_time_1 = df_all['window'][len(df_all)-1]["end"]-datetime.timedelta(minutes=4)
        start_time_1 = df_all['window'][len(df_all)-1]["start"]-datetime.timedelta(minutes=4)
        df_all = df_all[df_all["end_time"] >= end_time_1].reset_index().iloc[:,1:]
        x_all = list(set(x['start'] for x in df_all['window'].to_list()))
        x_all.sort()
        x_all = [x.strftime("%H:%M:%S") for x in x_all]
        y_1_1 = df_all[df_all["KEY"]==1]["COUNT"].to_list()
        y_1_2 = df_all[df_all["KEY"]==2]["COUNT"].to_list()
        y_1_3 = df_all[df_all["KEY"]==3]["COUNT"].to_list()
        y_2_1 = df_all[df_all["KEY"]==1]["ACCURACY"].to_list()
        y_2_2 = df_all[df_all["KEY"]==2]["ACCURACY"].to_list()
        y_2_3 = df_all[df_all["KEY"]==3]["ACCURACY"].to_list()
        ax1.clear()
        ax1.plot(x_all, y_1_1, '-b',label="KEY == 1")
        ax1.plot(x_all, y_1_2, '-y',label="KEY == 2")
        ax1.plot(x_all, y_1_3, '-r',label="KEY == 3")
        ax1.set_xlabel('Time')
        ax1.set_ylabel('Value')
        ax2.clear()
        ax2.plot(x_all, y_2_1, '-b',label="KEY == 1")
        ax2.plot(x_all, y_2_2, '-y',label="KEY == 2")
        ax2.plot(x_all, y_2_3, '-r',label="KEY == 3")
        ax1.set_xticks(x_all)
        ax1.set_xlabel('Time')
        ax1.set_ylabel('Value')
        leg = ax1.legend()
        fig.canvas.draw()

    else:
        if a == True:
            print("Still collecting more data, please wait {} seconds".format(120-(datetime.datetime.now()-now_time).total_seconds()))
            a = False
        if int(120-(datetime.datetime.now()-now_time).total_seconds())%40 < 1:
            print("Still collecting more data, please wait {} seconds".format(120-(datetime.datetime.now()-now_time).total_seconds()))
    time.sleep(5)


(datetime.datetime.now()-now_time).total_seconds()


fig.show()



consumer.poll()
