from typing import AnyStr
from confluent_kafka import Consumer
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.streaming import StreamingContext
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import numpy as np

spark = SparkSession.builder.appName("AppName").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

def consumer_act():
    c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'testing',
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest'
    })
    my_topic = "test" #same topic as producer
    c.subscribe([my_topic])
    #number_of_rows, cont, batch_size = 1418383, 0, 10000
    number_of_rows, cont, batch_size = 1000, 0, 100 #small test case
    batch_num = 1
    batch = []
    preBatch = []
    tb = []
    while cont < number_of_rows:
        msg = c.poll(1.0) #reads only one message
        if msg is None: #couldn't read anything
            print("NONE")
            continue
        elif msg.error():
            print("Consumer error: {0}".format(msg.error()))
            continue
        else:
            msg_val = msg.value().decode('utf-8').strip().split(",") #decode the utf8 and aplits the message into columns
            for i in range(15):#turns every column into int
                #msg_val[i] = int(float(msg_val[i]))
                msg_val[i] = float(msg_val[i])
            batch.append(msg_val)#adds the message to the batch
            remaining = number_of_rows - (cont + 1)
            if len(batch) == batch_size or (len(batch) == number_of_rows%batch_size and remaining < batch_size):
                #send batch to create the dataframe, return spark dataframe
                print("Batch", batch_num)#prints the number of the batch in grups of 10
                G = batch_processing(batch)
                if batch_num%3 != 0:#7 out of 10 are for training
                    preBatch.append(G)
                else: 
                    tb.append(G) #3 out of 10 are for evaluating
                batch_num += 1
                if batch_num > 10:
                    print("##10##")
                    batch_num = 1
                batch = []
        cont += 1
    metrics = []
    for b in preBatch:
        metrics.append(predictions(b,tb[0]))#computes randomForest with every batch and saves metrics
    compare(metrics)
    c.close()

def batch_processing(batch):
    global spark
    #crates spark scheme
    #cSchema = StructType([StructField("Arrest", IntegerType()), StructField("Domestic", IntegerType()), StructField("Beat",  IntegerType()), StructField("District", IntegerType()), StructField("Community_Area",  IntegerType()), StructField("X_Coordinate", IntegerType()), StructField("Y_Coordinate",  IntegerType()), StructField("IUCR",  IntegerType()), StructField("Loc_Descip_Index",  IntegerType()), StructField("FBI_Cod",  IntegerType()), StructField("Block_index",  IntegerType()), StructField("mes", IntegerType()), StructField("dia", IntegerType()), StructField("hora",  IntegerType()), StructField("minuto",  IntegerType())])
    cS = StructType([StructField("Arrest", DoubleType()), StructField("Domestic", DoubleType()), StructField("Beat",  DoubleType()), StructField("District", DoubleType()), StructField("Community_Area",  DoubleType()), StructField("X_Coordinate", DoubleType()), StructField("Y_Coordinate",  DoubleType()), StructField("IUCR",  DoubleType()), StructField("Loc_Descip_Index",  DoubleType()), StructField("FBI_Cod",  DoubleType()), StructField("Block_index",  DoubleType()), StructField("mes", DoubleType()), StructField("dia", DoubleType()), StructField("hora",  DoubleType()), StructField("minuto",  DoubleType())])
    df = spark.createDataFrame(batch, schema=cS)

    assemblerAtributos= VectorAssembler(inputCols=["Domestic", "Beat", "District", "Community_Area", "X_Coordinate", "Y_Coordinate", "IUCR", "Loc_Descip_Index", "FBI_Cod", "Block_index", "mes", "dia", "hora", "minuto"], outputCol= "Atributos")
    dfM = assemblerAtributos.transform(df)
    dfM = dfM.select("Atributos","Arrest")#Arrest is our target
    return dfM

def predictions(train,test):
    evaluator = BinaryClassificationEvaluator()
    rf = RandomForestClassifier(featuresCol = 'Atributos', labelCol = "Arrest")
    rfModel = rf.fit(train)
    predictionsRf = rfModel.transform(test)
    results = predictionsRf.select("Arrest", "prediction")
    predictionAndLabels = results.rdd
    metrics = MulticlassMetrics(predictionAndLabels)
    accuracy = metrics.accuracy
    #precision = metrics.precision(1.0)
    #recall = metrics.recall(2.0)
    #f1 = metrics.fMeasure(0.0, 2.0)
    #print("Test Area Under ROC: " + str(evaluator.evaluate(predictionsRf, {evaluator.metricName: "prediction"})))#any metric other than accuracy doesn't work
    print("Accuracy", accuracy)
    return [accuracy]

def compare(metrics):
    """
    input: matrix of metrics of the batches
    output:  None
    Description: prints the average for each metrics, also print the max for each metric
    """
    metrics = np.array(metrics)
    metricsMean = np.mean(metrics, axis=0)
    metricsMax = np.max(metrics, axis=0)
    print("metricsMean: ", metricsMean)
    print("metricsMax: ", metricsMax)

consumer_act()
spark.stop()