# new lib for part II
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.stat import Correlation
from pyspark.sql.types import IntegerType, StructType,StructField, StringType
from pyspark.ml.classification import DecisionTreeClassifier, GBTClassifier
from pyspark.ml.classification import NaiveBayes
import itertools
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

'''

██████╗░░█████╗░██████╗░████████╗  ██╗██╗
██╔══██╗██╔══██╗██╔══██╗╚══██╔══╝  ██║██║
██████╔╝███████║██████╔╝░░░██║░░░  ██║██║
██╔═══╝░██╔══██║██╔══██╗░░░██║░░░  ██║██║
██║░░░░░██║░░██║██║░░██║░░░██║░░░  ██║██║
╚═╝░░░░░╚═╝░░╚═╝╚═╝░░╚═╝░░░╚═╝░░░  ╚═╝╚═╝

--------------Feature extraction and ML Training--------------
'''
''' -------------------------- 2.1 Discuss the feature selection and prepare the feature columns -------------------------- '''

## flightsDf

len(flightsDf.columns)
pickleRdd = sc.pickleFile("flightsDf").collect()
flightsDf = spark.createDataFrame(pickleRdd)
del pickleRdd
## Categorical to numerical by one OneHotEncoder

labels = ["binaryArrDelay", "binaryDeptDelay", "multiClassArrDelay", "multiCassDeptDelay"]
categorical_features = [x for x in categorical_columns if x not in labels]
indexer = StringIndexer(inputCols=categorical_features+labels, outputCols = [x+"_index" for x in categorical_features+labels])
encoder = OneHotEncoder(inputCols=[x+"_index" for x in categorical_features if x not in ["DIVERTED", "YEAR", "CANCELLED"]],
          outputCols=[x+"_oneHot" for x in categorical_features if x not in ["DIVERTED", "YEAR", "CANCELLED"]])
features_cols = [x+"_oneHot" for x in categorical_features if x not in ["DIVERTED", "YEAR", "CANCELLED"]] + ["YEAR_index", "DIVERTED_index", "CANCELLED_index"] + numerical_columns
pipline_lst = [indexer, encoder]
pipeline = Pipeline(stages = pipline_lst)
pipelineModel = pipeline.fit(flightsDf)

df_trans = pipelineModel.transform(flightsDf)
df_trans.show()

## Manipulation correlation

# Trial
assembler = VectorAssembler(inputCols=[features_cols[4], "binaryArrDelay"], outputCol="corr")
m1 = assembler.transform(df_trans)
m1.printSchema()
mat = Correlation.corr(m1, "corr")
aa = pd.DataFrame(pd.DataFrame(mat.collect()[0]["pearson({})".format("corr")].values.reshape((6691, 6691))).iloc[:-1,-1])
aa.columns = ["corr"]

aa.insert(loc=0, column='Feature_Category', value=list(itertools.repeat("FLIGHT_NUMBER",6690)))
aa[abs(aa)>0.25]

# Work from here
pd_df_corr = pd.DataFrame(columns=["feature","corr"])
for feature_col in features_cols:
    assembler = VectorAssembler(inputCols=[feature_col, "binaryArrDelay"], outputCol="corr")
    m1 = assembler.transform(df_trans)
    mat = Correlation.corr(m1, "corr")
    len_mat = int(len(mat.collect()[0]["pearson({})".format("corr")].values)**0.5)
    aa = pd.DataFrame(pd.DataFrame(mat.collect()[0]["pearson({})".format("corr")].values.reshape((len_mat, len_mat))).iloc[:-1,-1])
    aa.columns = ["corr"]
    aa.insert(loc=0, column='feature', value=list(itertools.repeat(feature_col,len_mat-1)))
    pd_df_corr = pd.concat([pd_df_corr, aa])


pd_df_corr[abs(pd_df_corr["corr"])>0.01]

# 2. Subset of data
feature_columns = ["MONTH","DAY","DAY_OF_WEEK","AIRLINE","ORIGIN_AIRPORT","DESTINATION_AIRPORT","DEPARTURE_DELAY",
                    "DEPARTURE_TIME","TAXI_OUT","WHEELS_OFF","ELAPSED_TIME","TAXI_IN","SCHEDULED_ARRIVAL",
                    "ARRIVAL_TIME","ARRIVAL_DELAY"]
data_for_next = flightsDf.select(feature_columns+labels)

''' -------------------------- 2.2 Preparing any Spark ML Transformers/ Estimators for features and models -------------------------- '''

## 1 Create pipeline for the transformer

lst_indexer = [x+"_index" for x in feature_columns if x in categorical_features]
indexer = StringIndexer(inputCols=[x for x in feature_columns if x in categorical_features], outputCols = lst_indexer)
lst_encoder = [x+"_oneHot" for x in feature_columnS if x in categorical_features]
encoder = OneHotEncoder(inputCols=lst_indexer, outputCols=lst_encoder)
lst_numeric_cols = [x for x in feature_columns if x not in categorical_features]
all_features_cols = lst_encoder + lst_numeric_cols
assembler = VectorAssembler(inputCols = all_features_cols, outputCol = "features")
label_indexer = StringIndexer(inputCols = labels, outputCols = [x+"_index" for x in labels])
lst_pipeline = [indexer, encoder, assembler, label_indexer]
pipeline = Pipeline(stages = lst_pipeline)

# fit pipeline
pipelineModel = pipeline.fit(data_for_next)
df_ml = pipelineModel.transform(data_for_next)
df_ml.select("features").show()

## 2. Create seasons

data_for_next = data_for_next.withColumn("SEASON",
                    when((col("MONTH") < 6) & (col("MONTH") > 2),"Spring").otherwise(when((col("MONTH") < 9) & (col("MONTH") > 5),
                    "Summer").otherwise(when((col("MONTH") < 12) & (col("MONTH") > 8),"Autumn").otherwise("Winter"))))

feature_columns = ["MONTH","SEASON","DAY","DAY_OF_WEEK","AIRLINE","ORIGIN_AIRPORT","DESTINATION_AIRPORT",
                    "DEPARTURE_TIME","TAXI_OUT","WHEELS_OFF","ELAPSED_TIME","TAXI_IN","SCHEDULED_ARRIVAL",
                    "ARRIVAL_TIME"]

# Rebuild the transformer

lst_indexer = [x+"_index" for x in feature_columns if x in categorical_features + ["SEASON"]]
indexer = StringIndexer(inputCols=[x for x in feature_columns if x in categorical_features + ["SEASON"]], outputCols = lst_indexer)
lst_encoder = [x+"_oneHot" for x in feature_columns if x in categorical_features + ["SEASON"]]
encoder = OneHotEncoder(inputCols=lst_indexer, outputCols=lst_encoder)
lst_numeric_cols = [x for x in feature_columns if x not in categorical_features][1:]
all_features_cols = lst_encoder + lst_numeric_cols
assembler = VectorAssembler(inputCols = all_features_cols, outputCol = "features")
label_indexer = StringIndexer(inputCols = labels, outputCols = [x+"_index" for x in labels])
lst_pipeline = [indexer, encoder, assembler, label_indexer]
pipeline = Pipeline(stages = lst_pipeline)

# Re-fit
pipelineModel = pipeline.fit(data_for_next)
df_ml = pipelineModel.transform(data_for_next)
df_ml.select("features").show()

## 3. Create DT and GBT model

dt_arr = DecisionTreeClassifier(labelCol="binaryArrDelay_index", featuresCol="features")
dt_dep = DecisionTreeClassifier(labelCol="binaryDeptDelay_index", featuresCol="features")
gbt_arr = GBTClassifier(labelCol="binaryArrDelay_index", featuresCol="features", maxIter=10)
gbt_dep = GBTClassifier(labelCol="binaryDeptDelay_index", featuresCol="features", maxIter=10)

## 4. Naive Bayes

nb_arr = NaiveBayes(labelCol="multiClassArrDelay_index", featuresCol="features")
nb_dep = NaiveBayes(labelCol="multiCassDeptDelay_index", featuresCol="features")

## 5 Pipeline

lst_pipeline = [indexer, encoder, assembler, label_indexer]
pipeline = Pipeline(stages = lst_pipeline)

''' -------------------------- 2.3 Preparing the training and testing data -------------------------- '''

data_ready = pipeline.fit(data_for_next).transform(data_for_next)
(trainingData, testData) = data_ready.randomSplit([0.8, 0.2], seed=2021)

''' -------------------------- 2.4 Training and evaluating models -------------------------- '''

## 1. Binary classification

# a. Write code to use the corresponding ML Pipelines to train the models on the training data.
dt_arr_model = dt_arr.fit(trainingData)
dt_dep_model = dt_dep.fit(trainingData)
gbt_arr_model = gbt_arr.fit(trainingData)
gbt_dep_model = gbt_dep.fit(trainingData)

# b. For both models and for both delays, write code to display the count of
#    each combination of late/not late label and prediction label in formats as shown in
#    the example Fig.1

# dt_arr
dt_arr_prediction = dt_arr_model.transform(testData)
dt_arr_prediction.show()
dt_arr_prediction.printSchema()
dt_arr_prediction.groupBy("binaryArrDelay_index","prediction").count().show()

# dt_dep
dt_dep_prediction = dt_dep_model.transform(testData)
dt_dep_prediction.groupBy("binaryDeptDelay_index","prediction").count().show()

# gbt_arr

gbt_arr_prediction = gbt_arr_model.transform(testData)
gbt_arr_prediction.groupBy("binaryArrDelay_index","prediction").count().show()

# gbt_dep

gbt_dep_prediction = gbt_dep_model.transform(testData)
gbt_dep_prediction.groupBy("binaryArrDelay_index","prediction").count().show()

# c. AUC, accuracy, recall

# AUC

evaluator = BinaryClassificationEvaluator(labelCol = "binaryArrDelay_index", rawPredictionCol="rawPrediction")
auc_dt = evaluator.evaluate(dt_arr_prediction)
print(auc_dt)

evaluator = BinaryClassificationEvaluator(labelCol = "binaryDeptDelay_index", rawPredictionCol="rawPrediction")
auc_dt = evaluator.evaluate(dt_dep_prediction)
print(auc_dt)

evaluator = BinaryClassificationEvaluator(labelCol = "binaryArrDelay_index", rawPredictionCol="rawPrediction")
auc_dt = evaluator.evaluate(gbt_arr_prediction)
print(auc_dt)

evaluator = BinaryClassificationEvaluator(labelCol = "binaryDeptDelay_index", rawPredictionCol="rawPrediction")
auc_dt = evaluator.evaluate(gbt_dep_prediction)
print(auc_dt)

# accuracy and recall

def compute_metrics(predictions):
    TN = predictions.filter('prediction = 0 AND label = 0').count()
    TP = predictions.filter('prediction = 1 AND label = 1').count()
    FN = predictions.filter('prediction = 0 AND label = 1').count()
    FP = predictions.filter('prediction = 1 AND label = 0').count()
    accuracy = (TP+TN)/(TP+TN+FP+FN)
    precision =(TP)/(TP+FP)
    recall = (TP)/(TP+FN)
    f1 = 2/((1/recall)+(1/precision))
    return accuracy,precision,recall,f1

compute_metrics(dt_arr_prediction)
compute_metrics(dt_dep_prediction)
compute_metrics(gbt_arr_prediction)
compute_metrics(gbt_dep_prediction)

# d. and e Discuss

# f. leaf node and top-3 features

print(dt_arr_model.toDebugString)
# Use this to find the feature name
list_extract = []
for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
    list_extract = list_extract + dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]


# importance
featuresCol = "features"

dataset = trainingData

def ExtractFeatureImp(featureImp, dataset, featuresCol):
    list_extract = []
    for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
        list_extract = list_extract + dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
    varlist = pd.DataFrame(list_extract)
    varlist['score'] = varlist['idx'].apply(lambda x: featureImp[x])
    return(varlist.sort_values('score', ascending = False))

ExtractFeatureImp(dt_arr_model.featureImportances, trainingData, "features").head(3)


# g. discuss



## 2. multiclass classification
# train the model
nb_arr_model = nb_arr.fit(trainingData)
nb_dep_model = nb_dep.fit(trainingData)

# predict
nb_arr_prediction = nb_arr_model.transform(testData)

nb_dep_prediction = nb_dep_model.transform(testData)

nb_arr_prediction.groupBy("multiClassArrDelay_index","prediction").count().orderBy(["multiClassArrDelay_index","prediction"],ascending=False).show()

nb_dep_prediction.groupBy("multiCassDeptDelay_index","prediction").count().orderBy(["multiCassDeptDelay_index","prediction"],ascending=False).show()

# c AUC, recall, precision

# AUC
evaluator1 = MulticlassClassificationEvaluator(labelCol="multiClassArrDelay_index", predictionCol="prediction", metricName="accuracy")
auc = evaluator1.evaluate(nb_arr_prediction)

# OTHER

evaluator2 = MulticlassClassificationEvaluator(labelCol="multiClassArrDelay_index", predictionCol="prediction", metricName="weightedPrecision")
precision = evaluator2.evaluate(nb_arr_prediction)
evaluator3 = MulticlassClassificationEvaluator(labelCol="multiClassArrDelay_index", predictionCol="prediction", metricName="weightedRecall")
recall = evaluator3.evaluate(nb_arr_prediction)



#
