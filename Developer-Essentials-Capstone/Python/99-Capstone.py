# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Databricks Partner Capstone Project
# MAGIC 
# MAGIC This capstone is designed to review and validate key topics related to Databricks, Structured Streaming, and Delta. 
# MAGIC 
# MAGIC Upon successful completion of the capstone, you will receive a certificate of accreditation. Successful completion will be tracked alongside your partner profile, and will help our team identify individuals qualified for additional advanced training opportunities.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> In order for our tracking system to successfully log your completion, you will need to make sure you successfully run all 5 `realityCheck` functions in a single session.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Capstone Overview
# MAGIC 
# MAGIC In this project you will implement a multi-hop Delta Lake architecture using Spark Structured Streaming.
# MAGIC 
# MAGIC This architecture allows actionable insights to be derived from validated data in a data lake. Because Delta Lake provides ACID transactions and enforces schema, customers can build systems around reliable, available views of their data stored in economy cloud object stores.
# MAGIC 
# MAGIC ## Scenario:
# MAGIC 
# MAGIC A video gaming company stores historical data in a data lake, which is growing exponentially. 
# MAGIC 
# MAGIC The data isn't sorted in any particular way (actually, it's quite a mess) and it is proving to be _very_ difficult to query and manage this data because there is so much of it.
# MAGIC 
# MAGIC Your goal is to create a Delta pipeline to work with this data. The final result is an aggregate view of the number of active users by week for company executives. You will:
# MAGIC * Create a streaming Bronze table by streaming from a source of files
# MAGIC * Create a streaming Silver table by enriching the Bronze table with static data
# MAGIC * Create a streaming Gold table by aggregating results into the count of weekly active users
# MAGIC * Visualize the results directly in the notebook
# MAGIC 
# MAGIC ## Testing your Code
# MAGIC There are 5 test functions imported into this notebook:
# MAGIC * `realityCheckBronze(..)`
# MAGIC * `realityCheckStatic(..)`
# MAGIC * `realityCheckSilver(..)`
# MAGIC * `realityCheckGold(..)`
# MAGIC * `realityCheckFinal()`
# MAGIC 
# MAGIC To run automated tests against your code, you will call a `realityCheck` function and pass the function you write as an argument. The testing suite will call your functions against a different dataset so it's important that you don't change the parameters in the function definitions. 
# MAGIC 
# MAGIC To test your code yourself, simply call your function, passing the correct arguments. 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> Calling your functions will start a stream. Streams can take around 30 seconds to start so the tests may take up to one minute to run as it has to wait for the stream you define to start. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our environment and install our datasets into your workspace.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> You can force a reinstall (download) of the source datasets by setting reinstall to "false" in the following cell.

# COMMAND ----------

# MAGIC %run "./Includes/Capstone-Setup" $reinstall="false"

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Set up paths
# MAGIC 
# MAGIC The cell below sets up relevant paths in DBFS.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> It also clears out this directory (to ensure consistent results if re-run). This operation can take several minutes.

# COMMAND ----------

lookupSourcePath = f"{working_dir}/lookup_data"
eventSourcePath = f"{working_dir}/event_source"

print("Source Paths:")
print(lookupSourcePath)
print(eventSourcePath)

outputPath = f"{working_dir}/output"
dbutils.fs.rm(outputPath, True)

outputPathBronze = f"{outputPath}/bronze"
outputPathSilver = f"{outputPath}/silver"
outputPathGold   = f"{outputPath}/gold"

print("\nOutput Paths:")
print(outputPathBronze)
print(outputPathSilver)
print(outputPathGold)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### SQL Database Setup
# MAGIC 
# MAGIC We can avoid conflicts with other users in this workspace by using a personal database.
# MAGIC 
# MAGIC The follow cell drops that database, recreates it, and sets it as the default DB for future opreations.
# MAGIC 
# MAGIC Dropping the database also serves to "reset" the project much in the same way we deleted the output directory in the previous cell.

# COMMAND ----------

print(f"Setting up the database {user_db}")
spark.sql(f"DROP DATABASE IF EXISTS {user_db} CASCADE") # Drop the existing database
spark.sql(f"CREATE DATABASE {user_db}")                 # Recreate the database
spark.sql(f"USE {user_db}")                             # Use the database for all default table operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Prepare Schema and Read Streaming Data from input source
# MAGIC 
# MAGIC The input source is a small folder of JSON files. The provided logic is configured to read one file per trigger. 
# MAGIC 
# MAGIC Run this code to configure your streaming read on your file source. Because of Spark's lazy evaluation, a stream will not begin until we call an action on the `gamingEventDF` DataFrame.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> While the schema is provided for you, make sure that you note the nested nature of the `eventParams` field.

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, DoubleType, StructField

eventSchema = StructType([
  StructField('eventName', StringType()), 
  StructField('eventParams', StructType([ 
    StructField('game_keyword', StringType()),
    StructField('app_name', StringType()),
    StructField('scoreAdjustment', IntegerType()),
    StructField('platform', StringType()),
    StructField('app_version', StringType()),
    StructField('device_id', StringType()),
    StructField('client_event_time', TimestampType()),
    StructField('amount', DoubleType())
  ]))
])     

gamingEventDF = (spark
  .readStream
  .schema(eventSchema) 
  .option('streamName','mobilestreaming_demo') 
  .option("maxFilesPerTrigger", 1) # treat each file as Trigger event
  .json(eventSourcePath)           # The path to our JSON events (defined above)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Write Stream to Bronze Table
# MAGIC 
# MAGIC Complete the `writeToBronze` function to perform the following tasks:
# MAGIC 
# MAGIC * Write the stream from `gamingEventDF` -- the stream defined above -- to a bronze Delta table in path defined by `outputPathBronze`.
# MAGIC * Convert the (nested) input column `client_event_time` to a date format and rename the column to `eventDate`
# MAGIC * Filter out records with a null value in the `eventDate` column
# MAGIC * Make sure you provide a checkpoint directory that is unique to this stream
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> Using `append` mode when streaming allows us to insert data indefinitely without rewriting already processed data.

# COMMAND ----------

# TODO

from pyspark.sql.functions import col, to_date

def writeToBronze(sourceDataframe, bronzePath, streamName):
  (sourceDataframe
    .withColumn(FILL_IN)
    .filter(col(FILL_IN)
            
    FILL_IN
            
    .option("checkpointLocation", f"{bronzePath}_checkpoint")
    .queryName(streamName)
    .outputMode("append") 
    .start(outputPathBronze)
  )

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Call your writeToBronze function
# MAGIC 
# MAGIC To start the stream, call your `writeToBronze` function in the cell below.

# COMMAND ----------

writeToBronze(gamingEventDF, outputPathBronze, "bronze_stream")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Check your answer 
# MAGIC 
# MAGIC Call the realityCheckBronze function with your writeToBronze function as an argument.

# COMMAND ----------

realityCheckBronze(writeToBronze)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 3a: Load static data for enrichment
# MAGIC 
# MAGIC Complete the `loadStaticData` function to perform the following tasks:
# MAGIC 
# MAGIC * Register a static lookup table to associate `deviceId` with `deviceType` (android or ios).
# MAGIC * While we refer to this as a lookup table, here we'll define it as a DataFrame. This will make it easier for us to define a join on our streaming data in the next step.
# MAGIC * Create `deviceLookupDF` by calling your loadStaticData function, passing `lookupSourcePath` as the source path.

# COMMAND ----------

# TODO
print(lookupSourcePath)

def loadStaticData(path):
  return FILL_IN

deviceLookupDF = FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Check your answer
# MAGIC 
# MAGIC Call the reaityCheckStatic function, passing your loadStaticData function as an argument. 

# COMMAND ----------

realityCheckStatic(loadStaticData)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 3b: Create a streaming silver Delta table
# MAGIC 
# MAGIC A silver table is a table that combines, improves, or enriches bronze data. 
# MAGIC 
# MAGIC In this case we will join the bronze streaming data with some static data to add useful information. 
# MAGIC 
# MAGIC #### Steps to complete
# MAGIC 
# MAGIC Complete the `bronzeToSilver` function to perform the following tasks:
# MAGIC * Create a new stream by joining `deviceLookupDF` with the bronze table stored at `outputPathBronze` on `deviceId`.
# MAGIC * Make sure you do a streaming read and write
# MAGIC * Your selected fields should be:
# MAGIC   - `device_id`
# MAGIC   - `eventName`
# MAGIC   - `client_event_time`
# MAGIC   - `eventDate`
# MAGIC   - `deviceType`
# MAGIC * **NOTE**: some of these fields are nested; alias them to end up with a flat schema
# MAGIC * Write to `outputPathSilver`
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> Don't forget to checkpoint your stream!

# COMMAND ----------

# TODO

from pyspark.sql.functions import col

def bronzeToSilver(bronzePath, silverPath, streamName, lookupDF):
  (spark.readStream
    .format("delta")
    .load(bronzePath)

    FILL_IN

    .writeStream 

    FILL_IN

    .start(silverPath))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Call your bronzeToSilver function
# MAGIC 
# MAGIC To start the stream, call your `bronzeToSilver` function in the cell below.

# COMMAND ----------

bronzeToSilver(outputPathBronze, outputPathSilver, "silver_stream", deviceLookupDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Check your answer 
# MAGIC 
# MAGIC Call the realityCheckSilver function with your bronzeToSilver function as an argument.

# COMMAND ----------

realityCheckSilver(bronzeToSilver)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 4a: Batch Process a Gold Table from the Silver Table
# MAGIC 
# MAGIC The company executives want to look at the number of **distinct** active users by week. They use SQL so our target will be a SQL table backed by a Delta Lake. 
# MAGIC 
# MAGIC The table should have the following columns:
# MAGIC - `WAU`: count of weekly active users (distinct device IDs grouped by week)
# MAGIC - `week`: week of year (the appropriate SQL function has been imported for you)
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> There are *at least* two ways to successfully calculate weekly average users on streaming data. If you choose to use `approx_count_distinct`, note that the optional keyword `rsd` will need to be set to `.01` to pass the final check `Returns the correct DataFrame`.

# COMMAND ----------

# TODO

from pyspark.sql.functions import weekofyear

def silverToGold(silverPath, goldPath, queryName):
  FILL_IN

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Call your silverToGold function
# MAGIC 
# MAGIC To start the stream, call your `silverToGold` function in the cell below.

# COMMAND ----------

silverToGold(outputPathSilver, outputPathGold, "gold_stream")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Check your answer
# MAGIC 
# MAGIC Call the reaityCheckGold function, passing your silverToGold function as an argument. 

# COMMAND ----------

realityCheckGold(silverToGold)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Step 4b: Register Gold SQL Table
# MAGIC 
# MAGIC By linking the Spark SQL table with the Delta Lake file path, we will always get results from the most current valid version of the streaming table.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> It may take some time for the previous streaming operations to start. 
# MAGIC 
# MAGIC Once they have started register a SQL table against the gold Delta Lake path. 
# MAGIC 
# MAGIC * tablename: `mobile_events_delta_gold`
# MAGIC * table Location: `outputPathGold`

# COMMAND ----------

# TODO
spark.sql("""
   CREATE TABLE IF NOT EXISTS mobile_events_delta_gold
   FILL_IN
  """.format(outputPathGold))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 4c: Visualization
# MAGIC 
# MAGIC The company executives are visual people: they like pretty charts.
# MAGIC 
# MAGIC Create a bar chart out of `mobile_events_delta_gold` where the horizontal axis is month and the vertical axis is WAU.
# MAGIC 
# MAGIC Under <b>Plot Options</b>, use the following:
# MAGIC * <b>Keys:</b> `week`
# MAGIC * <b>Values:</b> `WAU`
# MAGIC 
# MAGIC In <b>Display type</b>, use <b>Bar Chart</b> and click <b>Apply</b>.
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/eLearning/Delta/plot-options-bar.png"/>
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> order by `week` to seek time-based patterns.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Wrap-up
# MAGIC 
# MAGIC * Stop streams

# COMMAND ----------

for s in spark.streams.active:
  s.stop()
  s.awaitTermination()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Submit Your Project
# MAGIC 
# MAGIC 0. Congrats for getting to the end of the capstone! 
# MAGIC 0. In order for the capstone to be properly evaluated ensure that this last reality check indicates that all four stages passed.
# MAGIC 0. If all four stages pass, your capstone project will be submitted for automatic processing.

# COMMAND ----------

realityCheckFinal()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
