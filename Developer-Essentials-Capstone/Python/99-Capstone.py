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
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> In order for our tracking system to successfully log your completion, you will need to make sure you successfully run all 4 `realityCheck` functions in a single session.
# MAGIC 
# MAGIC Certificates should arrive within a week of successful completion. **All tests must be passed successfully for certification**. If you have questions about your completion status, please email [training-enb@databricks.com](mailto:training-enb@databricks.com).

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
# MAGIC There are 4 test functions imported into this notebook:
# MAGIC * realityCheckBronze
# MAGIC * realityCheckStatic
# MAGIC * realityCheckSilver
# MAGIC * realityCheckGold
# MAGIC 
# MAGIC To run automated tests against your code, you will call a `realityCheck` function and pass the function you write as an argument. The testing suite will call your functions against a different dataset so it's important that you don't change the parameters in the function definitions. 
# MAGIC 
# MAGIC To test your code yourself, simply call your function, passing the correct arguments. 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> Calling your functions will start a stream. Streams can take around 30 seconds to start so the tests may take up to one minute to run as it has to wait for the stream you define to start. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enter your registration ID
# MAGIC 
# MAGIC You received a registration ID in an email when you enrolled into Databricks Core Technical Training Capstone. The title of the email that contains your registration ID is **Databricks Training Registration Success - Databricks Core Technical Training Capstone**. 
# MAGIC 
# MAGIC The email with the registration ID looks like this:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/core-capstone/reg_aws.png" width=60%/>
# MAGIC 
# MAGIC If you're unable to find your registration code in your email, you can also find it in your inbox in the [Databricks Academy](https://academy.databricks.com/) website. 
# MAGIC 
# MAGIC After logging in, click `MY ACCOUNT` in the top right:
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/common/academy_home.png" width=60%/>
# MAGIC 
# MAGIC Next, click on `Inbox` in the header:
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/common/academy_inbox.png" width=60%/>
# MAGIC 
# MAGIC Find the message titled **Databricks Training Registration Success - Databricks Core Technical Training Capstone**
# MAGIC 
# MAGIC The registration ID is in the body of the message.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/core-capstone/reg_aws_academy.png" width=60%/>
# MAGIC 
# MAGIC If you can't find the registration code using either method above, please send an email to [training-enb@databricks.com](mailto:training-enb@databricks.com). 
# MAGIC 
# MAGIC Enter your registration ID in the cell below as a string. This is a **critical** step to getting your accredidation for this capstone. 

# COMMAND ----------

# TODO

registration_id = FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Capstone-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Configure shuffle partitions
# MAGIC 
# MAGIC In order to speed up shuffle operations required by the solutions, let's update the number of shuffle partitions to 8 partitions. 

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Set up paths
# MAGIC 
# MAGIC The cell below sets up relevant paths in DBFS.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> It also clears out this directory (to ensure consistent results if re-run). This operation can take several minutes.

# COMMAND ----------

inputPath = userhome + "/source"

basePath = userhome + "/capstone"
outputPathBronze = basePath + "/gaming/bronze"
outputPathSilver = basePath + "/gaming/silver"
outputPathGold   = basePath + "/gaming/gold"

dbutils.fs.rm(basePath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### SQL Table Setup
# MAGIC 
# MAGIC The follow cell drops a table that we'll be creating later in the notebook.
# MAGIC 
# MAGIC (Dropping the table prevents challenges involved if the notebook is run more than once.)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS mobile_events_delta_gold;

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
  .option("maxFilesPerTrigger", 1)                # treat each file as Trigger event
  .json(inputPath) 
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
            
    .option("checkpointLocation", bronzePath + "/_checkpoint")
    .queryName(streamName)
    .outputMode("append") 
    .start(bronzePath)
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
# MAGIC * Create `deviceLookupDF` by calling your loadStaticData function, passing `/mnt/training/gaming_data/dimensionData` as the path.

# COMMAND ----------

# TODO
lookupPath = "/mnt/training/gaming_data/dimensionData"

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
# MAGIC # Double Check Your Submission
# MAGIC 
# MAGIC 
# MAGIC 1. Congrats for getting to the end of the capstone! 
# MAGIC 1. In order for the capstone to be properly evaluated, please **re-run the entire notebook and ensure that all reality checks pass**. 
# MAGIC 1. Once you have completed this step, you should receive an email with your badge within 2 weeks of competing the notebook.
# MAGIC 
# MAGIC ## Congratulations! You're all done!
# MAGIC 
# MAGIC You will receive your Databricks Developer Essential Badge within 2 weeks of successful completion of this capstone.  You will receive a notice about your digital badge via email and it can be downloaded through Accredible. Databricks has created a digital badge available in an online format so that you can easily retrieve and share the details of your achievement.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"/> In order for our tracking system to successfully log your completion, you will need to make sure you successfully run all 4 `realityCheck` functions in a single session. **Seriously, re-run your notebook! All tests must be passed successfully for certification**. If you have questions about your completion status, please submit a ticket [here](https://help.databricks.com/s/contact-us?ReqType=training) with the subject "Core Capstone". Please allow us 3-5 business days to respond. 

# COMMAND ----------

realityCheckFinal()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
