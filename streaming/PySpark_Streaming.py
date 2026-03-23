# Databricks notebook source
# MAGIC %md
# MAGIC # Pyspark Streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ## First Batch Process To Check
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Json Data

# COMMAND ----------

df=spark.read.format("json")\
    .option("inferSchema",True)\
    .option("multiLine",True)\
    .load("/Volumes/workspace/stream/tutstreaming/json_source")

# COMMAND ----------


display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Flatten the nested data
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df.select("order_id","timestamp","customer.customer_id","customer.email","customer.name","customer.address.city","customer.address.country","customer.address.postal_code").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explode array

# COMMAND ----------

df1 = df.select("order_id", "timestamp", "customer.customer_id", "customer.email", "customer.name", "customer.address.city",\
     "customer.address.country", "customer.address.postal_code", "items","metadata","payment")
df1 = df1.withColumn("items", explode_outer("items"))

display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### For the rest of the data

# COMMAND ----------


df1=df1.select(*[col(c) for c in df1.columns], *[col("items.item_id"), col("items.price"), col("items.product_name"), col("items.quantity")])
df1=df1.drop("Items")
df1.display()

# COMMAND ----------

df2=df1.select(*[col(c)for c in df1.columns],col("payment.method"),col("payment.transaction_id"))
df2.display()

# COMMAND ----------

df2=df2.drop("payment")
df2.display()

# COMMAND ----------

df3=df2.withColumn("metadata",explode_outer("metadata"))
df3.display()

# COMMAND ----------

df4=df3.select(*[col(c) for c in df3.columns],col("metadata.key"),col("metadata.value"))
df4.display()

# COMMAND ----------

df4=df4.drop("metadata")
df4.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Streaming Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Query

# COMMAND ----------

my_schema = '''
    order_id STRING,
    timestamp STRING,
    customer STRUCT<
        customer_id INT,
        name STRING,
        email STRING,
        address STRUCT<
            city STRING,
            country STRING,
            postal_code STRING
        >
    >,
    items ARRAY<STRUCT<
        item_id STRING,
        price DOUBLE,
        product_name STRING,
        quantity INT
    >>,
    payment STRUCT<
        method STRING,
        transaction_id STRING
    >,
    metadata ARRAY<STRUCT<
        key STRING,
        value STRING
    >>
'''

# COMMAND ----------


sdf = spark.readStream.format("json")\
    .option("multiLine", True)\
    .schema(my_schema)\
    .load("/Volumes/workspace/stream/tutstreaming/json_source")

# COMMAND ----------

df1 =sdf.select("order_id", "timestamp", "customer.customer_id", "customer.email", "customer.name", "customer.address.city",\
     "customer.address.country", "customer.address.postal_code", "items","metadata","payment")
df1 = df1.withColumn("items", explode_outer("items"))
df2=df1.select(*[col(c)for c in df1.columns],col("payment.method"),col("payment.transaction_id"))
df2=df2.drop("payment")
df3=df2.withColumn("metadata",explode_outer("metadata"))
df4=df3.select(*[col(c) for c in df3.columns],col("metadata.key"),col("metadata.value"))
df4=df4.drop("metadata")

# COMMAND ----------

df4.writeStream.format("delta")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("path", "/Volumes/workspace/stream/tutstreaming/jsonsink/Data")\
    .option("checkpointLocation", "/Volumes/workspace/stream/tutstreaming/jsonsink/checkpoint")\
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/stream/tutstreaming/jsonsink/Data`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming, Checkpoint, Triggers
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC While streaming if we add a file with the same name of a file that has been previously proccessed .Pyspark wont process that file again due to Idempotency

# COMMAND ----------

# MAGIC %md
# MAGIC ### Triggers Types

# COMMAND ----------

# MAGIC %md
# MAGIC - **Default** :Processes data as soon as it arrives, Latency: Minimal (depends on the batch duration).
# MAGIC Suitable for continuous, real-time streaming jobs.
# MAGIC - **Processing Time** :Processes data at a fixed time interval (e.g., every 5 seconds).
# MAGIC Latency: User-defined interval.
# MAGIC Useful for balancing resource usage and latency.
# MAGIC - **Once** :Processes all available data once and then stops.
# MAGIC Latency: Not applicable (batch-style processing).
# MAGIC Ideal for pipelines where streaming data is processed periodically like batch jobs.
# MAGIC - **Continuous** :A low-latency, experimental feature designed for sub-second processing.
# MAGIC Latency: Near real-time (sub-second).
# MAGIC Suitable for use cases with strict low-latency requirements.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Archiving

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/stream/tutstreaming/jsonsourcenew")
dbutils.fs.mkdirs("/Volumes/workspace/stream/tutstreaming/jsonsourcearchive")
dbutils.fs.mkdirs("/Volumes/workspace/stream/tutstreaming/jsonsinknew")



# COMMAND ----------

my_schema = '''
    order_id STRING,
    timestamp STRING,
    customer STRUCT<
        customer_id INT,
        name STRING,
        email STRING,
        address STRUCT<
            city STRING,
            country STRING,
            postal_code STRING
        >
    >,
    items ARRAY<STRUCT<
        item_id STRING,
        price DOUBLE,
        product_name STRING,
        quantity INT
    >>,
    payment STRUCT<
        method STRING,
        transaction_id STRING
    >,
    metadata ARRAY<STRUCT<
        key STRING,
        value STRING
    >>
'''

# COMMAND ----------


sdf = spark.readStream.format("json")\
    .option("multiLine", True)\
    .schema(my_schema)\
    .option("cleanSource","archive")\
    .option("sourceArchiveDir","/Volumes/workspace/stream/tutstreaming/jsonsourcearchive")\
    .load("/Volumes/workspace/stream/tutstreaming/jsonsourcenew")

df1 =sdf.select("order_id", "timestamp", "customer.customer_id", "customer.email", "customer.name", "customer.address.city",\
     "customer.address.country", "customer.address.postal_code", "items","metadata","payment")
df1 = df1.withColumn("items", explode_outer("items"))
df2=df1.select(*[col(c)for c in df1.columns],col("payment.method"),col("payment.transaction_id"))
df2=df2.drop("payment")
df3=df2.withColumn("metadata",explode_outer("metadata"))
df4=df3.select(*[col(c) for c in df3.columns],col("metadata.key"),col("metadata.value"))
df4=df4.drop("metadata")

# COMMAND ----------

df4.writeStream.format("delta")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("path", "/Volumes/workspace/stream/tutstreaming/jsonsinknew/Data")\
    .option("checkpointLocation", "/Volumes/workspace/stream/tutstreaming/jsonsinknew/checkpoint")\
    .start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explanation
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Archiving source files will be done when the file is processed from the source dir and then will be sent to archive dir .if the new file in soruce is a duplicate file it wont be archived.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Output Modes

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Types

# COMMAND ----------

# MAGIC %md
# MAGIC - **Append Mode**: Writes only new rows since the last trigger.only new rows generated since the last trigger are written to the sink. This mode is suitable for cases where no aggregation is required, or when results are incremental and don’t need updates.
# MAGIC - **Complete Mode**: Writes the entire aggregated result to the sink every time. the entire result table is written to the sink every time a trigger is executed. This is useful for streaming queries with aggregations, where you need to overwrite the output sink with updated results.
# MAGIC - **Update Mode**: Writes only the rows that were updated or changed since the last trigger.the entire result table is written to the sink every time a trigger is executed. This is useful for streaming queries with aggregations, where you need to overwrite the output sink with updated results.

# COMMAND ----------

# MAGIC %md
# MAGIC ### create delta table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

DeltaTable.createIfNotExists(spark)\
    .tableName("workspace.stream.sourcetable")\
        .addColumn("color","String")\
            .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert value

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workspace.stream.sourcetable VALUES
# MAGIC ("red"),("green"),("blue"),("yellow"),("orange"),("orange")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.stream.sourcetable

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/stream/tutstreaming/deltadata")

# COMMAND ----------

# MAGIC %md
# MAGIC ### read Stream

# COMMAND ----------

df=spark.readStream.table("workspace.stream.sourcetable")

# COMMAND ----------

# MAGIC %md
# MAGIC ### simple aggregation

# COMMAND ----------

df=df.groupBy("color").agg(count("*").alias("count"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### write stream

# COMMAND ----------

df.writeStream.format("Delta")\
    .outputMode("Complete")\
    .trigger(once=True)\
        .option("checkpointLocation","/Volumes/workspace/stream/tutstreaming/deltadata/check")\
        .option("path","/Volumes/workspace/stream/tutstreaming/deltadata/data")\
            .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from delta.`/Volumes/workspace/stream/tutstreaming/deltadata/data`

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workspace.stream.sourcetable VALUES
# MAGIC ("red"),("green"),("blue")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ForEachBatch

# COMMAND ----------

# MAGIC %md
# MAGIC ### create delta table and read stream

# COMMAND ----------

DeltaTable.createIfNotExists(spark)\
    .tableName("workspace.stream.foreachbatchsourcetable")\
        .addColumn("color","String")\
            .execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workspace.stream.foreachbatchsourcetable VALUES
# MAGIC ("red"),("green"),("blue"),("yellow"),("orange"),("orange")

# COMMAND ----------

df=spark.readStream.table("workspace.stream.foreachbatchsourcetable")

# COMMAND ----------

# MAGIC %md
# MAGIC ### define the function that treats each stream like a batch

# COMMAND ----------

def myfunc(df,batch_id):
    
    df=df.groupBy("color").agg(count("*").alias("count"))
    
  #destination 1
    df.write.format("Delta")\
        .mode("append")\
            .option("path","/Volumes/workspace/stream/tutstreaming/foreachbatchsink/dest1")\
            .save()
            
    #destination 2        
    df.write.format("Delta")\
        .mode("append")\
            .option("path","/Volumes/workspace/stream/tutstreaming/foreachbatchsink/dest2")\
                .save()
   

# COMMAND ----------

# MAGIC %md
# MAGIC ### writestream.foreachbatch

# COMMAND ----------

df.writeStream.foreachBatch(myfunc)\
    .outputMode("append")\
        .trigger(once=True)\
        .option("checkpointLocation","/Volumes/workspace/stream/tutstreaming/foreachbatchsink/check")\
            .start()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event Time Vs Processing Time

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explanation

# COMMAND ----------

# MAGIC %md
# MAGIC - **Processing Time** :It is simply the time recorded by the machine that is executing the job. It’s the clock on the server running your Spark application.
# MAGIC In the context of Structured Streaming, Processing Time is most commonly used to define your Trigger.
# MAGIC - **Event Time** :It is the timestamp recorded inside the data record itself, marking when the event actually occurred in the real world.
# MAGIC
# MAGIC **Processing Time (Trigger):** Handles the WHEN (When to run the code).
# MAGIC **Event Time (Watermark):** Handles the WHAT (What time window the data belongs to).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Windowing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tumbling Window

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774243422030.png](./image_1774243422030.png "image_1774243422030.png")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create delta tabble with timestamp ,Insert value and read stream

# COMMAND ----------

DeltaTable.createIfNotExists(spark)\
      .tableName("workspace.stream.windowtbl")\
        .addColumn("color","String")\
        .addColumn("event","TimeStamp")\
        .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workspace.stream.windowtbl VALUES
# MAGIC ("red",'2025-01-01T11:01:00.00+00:00'),("green",'2025-01-01T11:01:00.00+00:00')

# COMMAND ----------

df=spark.readStream.table("workspace.stream.windowtbl")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregate and WriteStream

# COMMAND ----------

df=df.groupby("color",window("event","10 minutes"))\
    .agg(count(lit(1)).alias("color_count"))

# COMMAND ----------

df.writeStream.format("Delta")\
    .outputMode("Complete")\
    .trigger(once=True)\
        .option("checkpointLocation","/Volumes/workspace/stream/tutstreaming/windows/check")\
        .option("path","/Volumes/workspace/stream/tutstreaming/windows/data")\
            .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from Delta.`/Volumes/workspace/stream/tutstreaming/windows/data`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add a new record after the window size

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workspace.stream.windowtbl VALUES
# MAGIC ("red",'2025-01-01T11:12:00.00+00:00'),("green",'2025-01-01T11:13:00.00+00:00')

# COMMAND ----------

df.writeStream.format("Delta")\
    .outputMode("Complete")\
    .trigger(once=True)\
        .option("checkpointLocation","/Volumes/workspace/stream/tutstreaming/windows/check")\
        .option("path","/Volumes/workspace/stream/tutstreaming/windows/data")\
            .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from Delta.`/Volumes/workspace/stream/tutstreaming/windows/data`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC The data wont be aggregated as the window size is different 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sliding Window

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774244776989.png](./image_1774244776989.png "image_1774244776989.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Session Window

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774245064417.png](./image_1774245064417.png "image_1774245064417.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Watermarking

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation

# COMMAND ----------

# MAGIC %md
# MAGIC **Watermarking** is a technique used to define the maximum delay allowed for late-arriving data in streaming pipelines. It lets Spark wait for a certain period before finalizing the results of a windowed aggregation. Once the watermark threshold is crossed, any data arriving late is ignored to prevent memory overflow or incorrect results.
# MAGIC
# MAGIC - Watermarks are typically applied to event time (not processing time).
# MAGIC - They help maintain a balance between accommodating late data and freeing up system resources.
# MAGIC - Spark drops data that is older than the watermark threshold.

# COMMAND ----------

