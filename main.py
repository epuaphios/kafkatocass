from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

#def load_json(txt):
#    try:
#        return json.loads(txt)
#    except Exception:
#        #print 'Message in topic is not JSON object! Got: ', txt
#        return {}
#
#def rename_sdf(df, mapper={}, **kwargs_mapper):
#
#    for before, after in mapper.items():
#        df = df.withColumnRenamed(before, after)
#    for before, after in kwargs_mapper.items():
#        df = df.withColumnRenamed(before, after)
#    return df


spark = SparkSession\
 .builder\
 .appName("kafka-spark-structured-stream")\
 .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0") \
 .config('spark.cassandra.connection.host', '172.18.72.150') \
 .config('spark.cassandra.connection.port', '9042') \
 .config('spark.cassandra.output.consistency.level','ONE') \
 .getOrCreate()

spark.sparkContext.setLogLevel("DEBUG")

schema = StructType() \
    .add('vehicleId', StringType(), True) \
    .add('vehicleType', StringType(), True) \
    .add('routeId', StringType(), True) \
    .add('longitude', StringType(), True) \
    .add('latitude', StringType(), True) \
    .add('timestamp', StringType(), True) \
    .add('speed', StringType(), True) \
    .add('fuelLevel', StringType(), True)

def writeToCassandra(writeDF, epochId):
    writeDF.write \
    .format("org.apache.spark.sql.cassandra")\
    .options(table="kafkatst", keyspace="example") \
    .mode("Append") \
    .save() 

#schema = StructField[(
#    StructField("vehicleId", StringType(), True),
#    StructField("vehicleType", IntegerType(), False),
#    StructField("routeId", StringType(), False),
#    StructField("longitude", IntegerType(), False),
#    StructField("latitude", IntegerType(), False),
#    StructField("timestamp", StringType(), False),
#    StructField("speed", IntegerType(), False),
#    StructField("fuelLevel", IntegerType(), False))]


df = spark\
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "172.18.72.133:9092,172.18.72.142:9092,172.18.72.149:9092") \
        .option("subscribe", "iot-data-event")\
        .option("startingOffsets", "earliest")\
        .option("group.id", "group-iot-data-4")\
        .load() \
        .selectExpr("cast(value as string)") \
        .select(from_json(col("value").cast("string"), schema).alias("parsed")) \
        .select("parsed.vehicleId","parsed.vehicleType","parsed.routeId","parsed.longitude","parsed.latitude","parsed.timestamp","parsed.speed","parsed.fuelLevel") \
        
df = (df
    .withColumnRenamed('vehicleId','vehicleid')
    .withColumnRenamed('vehicleType', 'vehicletype')
    .withColumnRenamed('routeId', 'routeid')
    .withColumnRenamed('fuelLevel', 'fuellevel'))
df.printSchema()
 
query =  df.writeStream \
        .trigger(processingTime="10 seconds") \
        .option('checkpointLocation', '/home/ogn/spark/chkpoint/') \
        .foreachBatch(writeToCassandra) \
        .outputMode("update") \
        .start()

query.awaitTermination()


    

#sc=spark.sqlContext
#df = sqlContext.read.json(sc.parallelize(df))
#df.show()

#for record in df:
#    sc=spark.sqlContext
#    stringJSONRDD = sc.parallelize(record)

#jsons_dstream = df.rdd.map(lambda x: load_json(x[1])).filter(lambda x: len(x)>0) #filter out non-json messages
#
# csv_dstream = jsons_dstream.map(lambda j_msg: '%s,%s,%s'%(j_msg.get('vehicleId'), j_msg.get('vehicleType'), j_msg.get('routeId')))
#print(csv_dstream)
#



