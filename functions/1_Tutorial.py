# Databricks notebook source
# MAGIC %md
# MAGIC ### DATA READING

# COMMAND ----------

dbutils.fs.ls('/Volumes/workspace/stream/streaming/')

# COMMAND ----------

df=spark.read.format('csv').option('inferSchema',True).option('header',True).load('/Volumes/workspace/stream/streaming/BigMart Sales.csv')


# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### READ JSON

# COMMAND ----------

df_json=spark.read.format('json').option('inferSchema',True)\
        .option('header',True).option('multiLine',False)\
        .load('/Volumes/workspace/stream/streaming/drivers.json')


# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###PRINT SCHEMA

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###DDL SCHEMA

# COMMAND ----------

my_ddl_schema='''
Item_Identifier STRING,
Item_Weight STRING, 
Item_Fat_Content STRING, 
Item_Visibility DOUBLE, 
Item_Type STRING, 
Item_MRP DOUBLE,
Outlet_Identifier STRING, 
Outlet_Establishment_Year INT, 
Outlet_Size STRING, 
Outlet_Location_Type STRING, 
Outlet_Type STRING, 
Item_Outlet_Sales DOUBLE 
'''

# COMMAND ----------

df=spark.read.format('csv')\
    .option('header',True)\
    .schema(my_ddl_schema)\
    .load('/Volumes/workspace/stream/streaming/BigMart Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###STRUCT TYPE

# COMMAND ----------

# DBTITLE 1,StructType and related imports
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema = StructType([
    StructField("Item_Identifier", StringType(), True),
    StructField("Item_Weight", StringType(), True),
    StructField("Item_Fat_Content", StringType(), True),
    StructField("Item_Visibility", DoubleType(), True),
    StructField("Item_Type", StringType(), True),
    StructField("Item_MRP", DoubleType(), True),
    StructField("Outlet_Identifier", StringType(), True),
    StructField("Outlet_Establishment_Year", DoubleType(), True),
    StructField("Outlet_Size", StringType(), True),
    StructField("Outlet_Location_Type", StringType(), True),
    StructField("Outlet_Type", StringType(), True),
    StructField("Item_Outlet_Sales", DoubleType(), True)
])

# COMMAND ----------

struct_df=spark.read.format('csv')\
    .option('header',True)\
    .schema(my_struct_schema)\
    .load('/Volumes/workspace/stream/streaming/BigMart Sales.csv')

# COMMAND ----------

struct_df.display()

# COMMAND ----------

struct_df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### NOW READ ORIGINAL DF

# COMMAND ----------

df=spark.read.format('csv')\
    .option('inferSchema',True).\
    option('header',True)\
    .load('/Volumes/workspace/stream/streaming/BigMart Sales.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ###SELECT

# COMMAND ----------

df_sel=df.select('Item_Identifier','Item_Fat_Content','Item_Weight').display()

# COMMAND ----------

df_sel=df.select(col('Item_Identifier'),col('Item_Fat_Content'),col('Item_Weight')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###ALIAS

# COMMAND ----------

df.select(col("Item_Identifier").alias('Item_Id')).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### FILTER/WHERE

# COMMAND ----------

# MAGIC %md
# MAGIC #### SCENARIO !: FILTER ROWS WHERE FAT CONTENT=REGULaR

# COMMAND ----------

df.filter(col("Item_Fat_Content")=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####SLICE THE DATA with item type=Soft drink and weight <10

# COMMAND ----------

df.filter((col('Item_Type')=='Soft Drinks') & (col('Item_Weight') < 10)).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ####FECTH the data in tier (tier 1 or tier 2) and outlet size is NULL

# COMMAND ----------

df.filter(((col('Outlet_Location_Type')=='Tier 1') |(col('Outlet_Location_Type')=='Tier 2')) & (col('Outlet_Size').isNull())).display()
#can use isin() so col('OUtlet ...).isin('Tier 1','Tier 2')

# COMMAND ----------

# MAGIC %md 
# MAGIC ####RENAME COLUMN withColumnRenamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_Wt')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### withColumn 

# COMMAND ----------

# MAGIC %md 
# MAGIC ####create new column wiht constant value using lit()

# COMMAND ----------

df.withColumn('flag',lit('new')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####  new column transformation
# MAGIC

# COMMAND ----------

df.withColumn('double_weight',col('Item_Weight')*2).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### regreplace

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

regrep_df=df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), 'Low Fat', 'LF '))\
.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), 'Regular', 'reg'))\
.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), 'low fat', 'LF '))\
.withColumn('Item_Fat_Content', trim(col("Item_Fat_Content")))



regrep_df.display()

# COMMAND ----------

regrep_df.groupBy("Item_Fat_Content").count().orderBy(desc("count")).display()


# COMMAND ----------

# MAGIC %md 
# MAGIC ###TYPE CASTING

# COMMAND ----------

df.withColumn('Item_Weight',col('Item_Weight').cast(StringType())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sort/orderBy

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### sorting multiple columns 

# COMMAND ----------

df.sort(['Item_Weight',"Item_Visibility"],ascending=[0,0]).display()# {0,0} means false so sorts those columns in descending order

# COMMAND ----------

df.sort(["Item_Weight","Item_Visibility"],ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LIMIT

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DROP

# COMMAND ----------

df.drop('Item_Weight','Item_Fat_Content').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####drop_duplicates

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.drop_duplicates(subset=['Item_Type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### create df

# COMMAND ----------

data1=[(1,'Ram'),
     (2,'Shyam')]
schema='ID INTEGER, NAME STRING'
data2=[(3,'Krishna'),
     (4,'Hari')]
df1=spark.createDataFrame(data1,schema)
df2=spark.createDataFrame(data2,schema)


# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###union unionByName

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data1=[(1,'Ram'),
     (2,'Shyam')]
schema1='ID string, NAME STRING'
schema2=' NAME string,  ID string'
data2=[('Krishna',3),
     ('Hari',4)]
df1=spark.createDataFrame(data1,schema1)
df2=spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### STring fucntions

# COMMAND ----------

# MAGIC %md 
# MAGIC #### initcap
# MAGIC
# MAGIC

# COMMAND ----------

df.select(initcap('Item_Type')).display()

# COMMAND ----------

df.select(lower('Item_Type')).display()


# COMMAND ----------

df.select(upper('Item_Type').alias("ITEM_TYPE")).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### DATE FUNCTIONS

# COMMAND ----------

c_df=df.withColumn("Curr_Date",current_date())
c_df.display()

# COMMAND ----------

c_df=c_df.withColumn("week_after",date_add('Curr_Date',7))
c_df.display()

# COMMAND ----------

c_df.withColumn('week_before',date_sub('CURR_DATE',7)).display()

# COMMAND ----------

c_df=c_df.withColumn("date_dff",datediff('week_after','CURR_DATE'))
c_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### handling nulls

# COMMAND ----------

c_df.dropna().display()#any means in any col ,#subset means in a col only and # all means in all col


# COMMAND ----------

c_df.fillna('NOT_AVAILABLE').display()

# COMMAND ----------

c_df.fillna(value=0,subset=['Item_Weight']).display()

# COMMAND ----------

import pyspark.pandas as pd
cc_df=pd.DataFrame(c_df)#change to pandas df
itm_Wt_mean=cc_df.Item_Weight.mean()#get mean
c_df.fillna(itm_Wt_mean,subset=['Item_Weight']).display()#fill na using pyspark fxn and spark df

# COMMAND ----------

# MAGIC %md
# MAGIC #### SPLIT

# COMMAND ----------

split_df=c_df
split_df=split_df.withColumn("Outlet_Type",split('Outlet_Type',' '))
split_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### indexing

# COMMAND ----------

c_df.withColumn('Outlet_Type',split('Outlet_type','')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### explode

# COMMAND ----------

split_df.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### array contains

# COMMAND ----------

split_df.withColumn('TYPE1_Flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### group_by

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 1: Sum and average of MRP of each ITEM type

# COMMAND ----------

df.groupBy("Item_Type").agg(sum('Item_MRP')).display()

# COMMAND ----------

df.groupby('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

drop_df=df.dropna()
drop_df.groupby('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('TOTAL_MRP')).display()

# COMMAND ----------

drop_df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP'),avg('Item_MRP').alias('Avg_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Collect list

# COMMAND ----------

sample=[
  ('user1','book1'),
  ('user2','book2'),
  ('user1','book3'),
  ('user2','book4'),
  ('user3','book5')
]
s_schema='USER STRING ,BOOK STRING'
book_df=spark.createDataFrame(sample,s_schema)
book_df.display()

# COMMAND ----------

book_df.groupby('USER').agg(collect_list('BOOK').alias('BOOKS_OWNED')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PIVOT

# COMMAND ----------

drop_df.groupby("Item_Type").pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### when-otherwise

# COMMAND ----------

when_df=drop_df.withColumn('Preference_flag', when(col('Item_Type')=='Meat', 'Non-veg').otherwise('Veg'))
when_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### multiple when
# MAGIC

# COMMAND ----------

when_df.withColumn('Flag_expensive',when(((col('Preference_flag')=='Veg')& (col('Item_MRP')>100)),'Veg_Expensive')\
  .when(((col('Preference_flag')=='Veg')& (col('Item_MRP')<100)),'Veg_INExpensive')\
  .otherwise('NON_VEG')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### joins

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###inner join

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'inner').display()

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### anti join

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],"anti").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### window functions

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC #### row_number

# COMMAND ----------


df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### rank vs dense_rank

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy('Item_Identifier')))\
  .withColumn('dense_rank',dense_rank().over(Window.orderBy(col('Item_Identifier'))))\
    .display()

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy('Item_Identifier'))).withColumn('desc_rank',rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

df.withColumn('denserank',dense_rank().over(Window.orderBy('Item_Identifier')))\
  .withColumn('desc_denserank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc())))\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### cumulative sum

# COMMAND ----------

df.withColumn('CUM_SUM',sum('Item_MRP').over(Window.orderBy('Item_Type')\
    .rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### UDF

# COMMAND ----------

def my_func(x):
  return x*x

# COMMAND ----------

my_udf=udf(my_func)

# COMMAND ----------

df.withColumn('udf_square',my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data writing

# COMMAND ----------

# MAGIC %md
# MAGIC #### csv

# COMMAND ----------

df.write.format('csv')\
    .save('/Volumes/workspace/stream/streaming/BigMart_Sales_Tutorial.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### writing modes

# COMMAND ----------

df.write.format('csv')\
    .mode('append')\
    .option('path','/Volumes/workspace/stream/streaming/BigMart_Sales_Tutorial.csv')\
    .save()

# COMMAND ----------

df.write.format('csv')\
    .mode('overwrite')\
    .option('path','/Volumes/workspace/stream/streaming/BigMart_Sales_Tutorial.csv')\
    .save()

# COMMAND ----------

df.write.format('csv')\
    .mode('error')\
    .option('path','/Volumes/workspace/stream/streaming/BigMart_Sales_Tutorial.csv')\
    .save()

# COMMAND ----------

df.write.format('csv')\
    .mode('ignore')\
    .option('path','/Volumes/workspace/stream/streaming/BigMart_Sales_Tutorial.csv')\
    .save()

# COMMAND ----------

df.write\
    .mode('overwrite')\
    .saveAsTable('BIGMART_SALESS')

# COMMAND ----------

# MAGIC %md
# MAGIC ### spark sql

# COMMAND ----------

df.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view;

# COMMAND ----------

sql_df=spark.sql("Select * from my_view where Item_Fat_Content ='Low Fat'")
sql_df.display()

# COMMAND ----------

