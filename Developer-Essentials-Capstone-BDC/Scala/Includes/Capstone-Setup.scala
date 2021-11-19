// Databricks notebook source
// MAGIC 
// MAGIC %run "./Course-Name"

// COMMAND ----------

// MAGIC %run "./Dataset-Mounts"

// COMMAND ----------

// MAGIC %run "./Test-Library"

// COMMAND ----------

// SETUP

import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, TimestampType, IntegerType}

val basePath = userhome + "/capstone"
dbutils.fs.rm(basePath, true)
val outputPathBronzeTest = basePath + "/gaming/bronzeTest"
val outputPathSilverTest = basePath + "/gaming/silverTest"
val outputPathGoldTest = basePath + "/gaming/goldTest"

lazy val eventSchema = StructType(List(
  StructField("eventName", StringType, true),
  StructField("eventParams", StructType(List(
    StructField("game_keyword", StringType, true),
    StructField("app_name", StringType, true),
    StructField("scoreAdjustment", IntegerType, true),
    StructField("platform", StringType, true),
    StructField("app_version", StringType, true),
    StructField("device_id", StringType, true),
    StructField("client_event_time", TimestampType, true),
    StructField("amount", DoubleType, true)
  )), true)
))

val singleFilePath = "/mnt/training/enb/capstone-data/single"

object Key {
  val singleStreamDF = (spark
  .readStream
  .schema(eventSchema) 
  .option("streamName","mobilestreaming_test") 
  .option("maxFilesPerTrigger", 1)                 
  .json(singleFilePath) 
  )

  val bronzeDF  = (spark
    .read
    .format("delta")
    .load("/mnt/training/enb/capstone-data/bronze") 
  )

  val correctLookupDF = (spark.read
    .format("delta")
    .load("/mnt/training/enb/capstone-data/lookup"))

  val silverDF = (spark
    .read
    .format("delta")
    .load("/mnt/training/enb/capstone-data/silver") 
  )

  val goldDF = (spark
    .read
    .format("delta") 
    .load("/mnt/training/enb/capstone-data/gold") 
  )
}

display(Seq())

// COMMAND ----------

// SOURCE
dbutils.fs.rm(userhome+"/source", true)
dbutils.fs.mkdirs(userhome+"/source")
dbutils.fs.cp("/mnt/training/gaming_data/mobile_streaming_events/part-00000-tid-6718866119967790308-cef1b03e-5fda-4259-885e-e992ca3996c3-25700-c000.json", userhome+"/source/file-0.json")
dbutils.fs.cp("/mnt/training/gaming_data/mobile_streaming_events/part-00001-tid-6718866119967790308-cef1b03e-5fda-4259-885e-e992ca3996c3-25701-c000.json", userhome+"/source/file-1.json")
dbutils.fs.cp("/mnt/training/gaming_data/mobile_streaming_events/part-00002-tid-6718866119967790308-cef1b03e-5fda-4259-885e-e992ca3996c3-25702-c000.json", userhome+"/source/file-2.json")

// COMMAND ----------

// BRONZE

def realityCheckBronze(writeToBronze: (org.apache.spark.sql.DataFrame, String, String)=>Unit, display:Boolean=true): Unit = {

  dbutils.fs.rm(outputPathBronzeTest, true)
  
  val bronze_tests = TestSuite()

  try {
  
    writeToBronze(Key.singleStreamDF, outputPathBronzeTest, "bronze_test")

    def groupAndCount(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
      return df.select("eventName").groupBy("eventName").count()
    }

    for (s <- spark.streams.active) {
      if (s.name == "bronze_test") {
        var first = true
          while (s.recentProgress.length == 0) {
            if (first) {
              println("waiting for stream to start...")
              first = false
            }
            Thread.sleep(5000)
          } 
      }     
    }
    
    var testDF: org.apache.spark.sql.DataFrame = null

    try {
      testDF = (spark
          .read
          .format("delta")
          .load(outputPathBronzeTest))
    }
    catch {
      case e : Throwable =>
        println(e)
        testDF = (spark
          .read
          .load(outputPathBronzeTest))
    } 

    val test_dtype = findColumnDatatype(testDF, "eventDate")

    val historyDF = spark.sql(s"DESCRIBE HISTORY delta.`$outputPathBronzeTest`")

    bronze_tests.test(id = "rc_bronze_delta_format", points = 2, description = "Is in Delta format", 
        testFunction = () =>  isDelta(outputPathBronzeTest))
    bronze_tests.test(id = "rc_bronze_contains_columns", points = 2, description = "Dataframe contains eventDate column",              
            testFunction = () =>  verifyColumnsExists(testDF, List("eventDate")))
    bronze_tests.test(id = "rc_bronze_correct_schema", points = 2, description = "Returns correct schema", 
            testFunction = () =>  checkSchema(testDF.schema, Key.bronzeDF.schema))
    bronze_tests.test(id = "rc_bronze_column_check", points = 2, description = "eventDate column is correct data type",              
            testFunction = () =>  test_dtype == "DateType")
    bronze_tests.test(id = "rc_bronze_null_check", points = 2, description = "Does not contain nulls",              
            testFunction = () =>  !(checkForNulls(testDF, "eventParams")))
    bronze_tests.test(id = "rc_bronze_is_streaming", points = 2, description = "Is streaming DataFrame",              
            testFunction = () =>  isStreamingDataframe(historyDF))
    bronze_tests.test(id = "rc_bronze_output_mode", points = 2, description = "Output mode is Append",              
            testFunction = () =>  checkOutputMode(historyDF, "Append"))
    bronze_tests.test(id = "rc_bronze_correct_rows", points = 2, description = "Returns a Dataframe with the correct number of rows",      
            testFunction = () =>  testDF.count() == Key.bronzeDF.count())
    bronze_tests.test(id = "rc_bronze_correct_df", points = 2, description = "Returns the correct Dataframe",             
            testFunction = () =>  compareDataFrames(groupAndCount(testDF), groupAndCount(Key.bronzeDF)))

    val bronze_results = f"""{"registration_id": ${registration_id}, "passed": ${bronze_tests.passed}, "percentage": ${bronze_tests.percentage}, "actPoints": ${bronze_tests.score}, "maxPoints": ${bronze_tests.maxScore}}"""
    
    daLogger.logEvent("Bronze Reality Check", bronze_results)
    
    if (display == true) bronze_tests.displayResults()
   
  }
  
  finally{
    for (s <- spark.streams.active) {
      if (s.name == "bronze_test") {
        try {
          s.awaitTermination(10)
          s.stop()
        }
        catch {
          case e : Throwable =>
            println("!!", e)
        }
      }
    }
  }
}

displayHTML("""
Declared the following function:
  <li><span style="color:green; font-weight:bold">realityCheckBronze</span></li>
""")

// COMMAND ----------

// STATIC

def realityCheckStatic(loadStaticData: (String) => org.apache.spark.sql.DataFrame, display:Boolean=true): Unit = {
  
  val static_tests = TestSuite()
  
  val testDF = loadStaticData("/mnt/training/enb/capstone-data/lookup")
  
  static_tests.test(id = "rc_static_count", points = 2, description = "Has the correct number of rows", 
             testFunction = () =>  testDF.count() == 475)
  static_tests.test(id = "rc_static_schema", points = 2, description = "Returns correct schema", 
               testFunction = () =>  checkSchema(testDF.schema, Key.correctLookupDF.schema))
  
  val static_results = f"""{"registration_id": ${registration_id}, "passed": ${static_tests.passed}, "percentage": ${static_tests.percentage}, "actPoints": ${static_tests.score}, "maxPoints": ${static_tests.maxScore}}"""

  daLogger.logEvent("Static Reality Check", static_results)
  
  if (display == true) static_tests.displayResults()
  
}

displayHTML("""
Declared the following function:
  <li><span style="color:green; font-weight:bold">realityCheckStatic</span></li>
""")

// COMMAND ----------

// SILVER

def realityCheckSilver(bronzeToSilver: (String, String, String, org.apache.spark.sql.DataFrame)=>Unit, display:Boolean=true): Unit = {

  dbutils.fs.rm(outputPathSilverTest, true)
  
  val silver_tests = TestSuite()

  try {
  
    bronzeToSilver(outputPathBronzeTest, outputPathSilverTest, "silver_test", Key.correctLookupDF)

    def groupAndCount(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
        return df.select("deviceType").groupBy("deviceType").count()
    }

    for (s <- spark.streams.active) {
      if (s.name == "silver_test") {
        var first = true
        while (s.recentProgress.length == 0) {
          if (first) {
            println("waiting for stream to start...")
            first = false
          }
          Thread.sleep(5000)
        } 
      }   
    }
    
    var testDF: org.apache.spark.sql.DataFrame = null

    try {
      testDF = (spark
          .read
          .format("delta")
          .load(outputPathSilverTest))
    }
    catch {
      case e : Throwable =>
        println(e)
        testDF = (spark
          .read
          .load(outputPathSilverTest))
    } 

    val historyDF = spark.sql(s"DESCRIBE HISTORY delta.`$outputPathSilverTest`")

    silver_tests.test(id = "rc_silver_delta_format", points = 2, description = "Is in Delta format", 
            testFunction = () =>  isDelta(outputPathSilverTest))
    silver_tests.test(id = "rc_silver_contains_columns", points = 2, description = "Dataframe contains device_id, client_event_time, deviceType columns",              
            testFunction = () =>  verifyColumnsExists(testDF, List("eventDate", "client_event_time", "deviceType")))
    silver_tests.test(id = "rc_silver_correct_schema", points = 2, description = "Returns correct schema", 
            testFunction = () =>  checkSchema(testDF.schema, Key.silverDF.schema))
    silver_tests.test(id = "rc_silver_null_check", points = 2, description = "Does not contain nulls",              
            testFunction = () =>  !(checkForNulls(testDF, "eventName")))
    silver_tests.test(id = "rc_silver_is_streaming", points = 2, description = "Is streaming DataFrame",              
            testFunction = () =>  isStreamingDataframe(historyDF))
    silver_tests.test(id = "rc_silver_output_mode", points = 2, description = "Output mode is Append",              
            testFunction = () =>  checkOutputMode(historyDF, "Append"))
    silver_tests.test(id = "rc_silver_correct_rows", points = 2, description = "Returns a Dataframe with the correct number of rows",              
            testFunction = () =>  testDF.count() == Key.silverDF.count())
    silver_tests.test(id = "rc_silver_correct_df", points = 2, description = "Returns the correct Dataframe",              
            testFunction = () =>  compareDataFrames(groupAndCount(testDF), groupAndCount(Key.silverDF)))

    val silver_results = f"""{"registration_id": ${registration_id}, "passed": ${silver_tests.passed}, "percentage": ${silver_tests.percentage}, "actPoints": ${silver_tests.score}, "maxPoints": ${silver_tests.maxScore}}"""

    daLogger.logEvent("Silver Reality Check", silver_results)

    if (display == true) silver_tests.displayResults()

  }
  
  finally{
    for (s <- spark.streams.active) {
      if (s.name == "silver_test") {
        try {
          s.stop()
        }
        catch {
          case e : Throwable =>
            println("!!", e)
        }
      }
    }
  }
}
  
displayHTML("""
Declared the following function:
  <li><span style="color:green; font-weight:bold">realityCheckSilver</span></li>
""")

// COMMAND ----------

// GOLD

def realityCheckGold(silverToGold: (String, String, String)=>Unit, display:Boolean=true): Unit = {

  dbutils.fs.rm(outputPathGoldTest, true)
  
  val gold_tests = TestSuite()

  try {
  
    silverToGold(outputPathSilverTest, outputPathGoldTest, "gold_test")

    for (s <- spark.streams.active) {
      if (s.name == "gold_test") {
        var first = true
        while (s.recentProgress.length == 0) {
          if (first) {
            println("waiting for stream to start...")
            first = false
          }
          Thread.sleep(5000)
        } 
      }   
    }
    
    var testDF: org.apache.spark.sql.DataFrame = null

    try {
      testDF = (spark
          .read
          .format("delta")
          .load(outputPathGoldTest))
    }
    catch {
      case e : Throwable =>
        println(e)
        testDF = (spark
          .read
          .load(outputPathGoldTest))
    } 

    val historyDF = spark.sql(s"DESCRIBE HISTORY delta.`$outputPathGoldTest`")

    gold_tests.test(id = "rc_gold_delta_format", points = 2, description = "Is in Delta format", 
             testFunction = () =>  isDelta(outputPathGoldTest))
    gold_tests.test(id = "rc_gold_contains_columns", points = 2, description = "Dataframe contains week and WAU columns",              
             testFunction = () =>  verifyColumnsExists(testDF, List("week", "WAU")))
    gold_tests.test(id = "rc_gold_correct_schema", points = 2, description = "Returns correct schema", 
             testFunction = () =>  checkSchema(testDF.schema, Key.goldDF.schema))
    gold_tests.test(id = "rc_gold_null_check", points = 2, description = "Does not contain nulls",              
             testFunction = () =>  !(checkForNulls(testDF, "week")))
    gold_tests.test(id = "rc_gold_is_streaming", points = 2, description = "Is streaming DataFrame",              
             testFunction = () =>  isStreamingDataframe(historyDF))
    gold_tests.test(id = "rc_gold_output_mode", points = 2, description = "Output mode is Complete",              
             testFunction = () =>  checkOutputMode(historyDF, "Complete"))
    gold_tests.test(id = "rc_gold_correct_rows", points = 2, description = "Returns a Dataframe with the correct number of rows",              
             testFunction = () =>  testDF.count() == Key.goldDF.count())
    gold_tests.test(id = "rc_gold_correct_df", points = 2, description = "Returns the correct Dataframe",              
             testFunction = () =>  compareDataFrames(testDF.sort("week"), Key.goldDF.sort("week")))

    val gold_results = f"""{"registration_id": ${registration_id}, "passed": ${gold_tests.passed}, "percentage": ${gold_tests.percentage}, "actPoints": ${gold_tests.score}, "maxPoints": ${gold_tests.maxScore}}"""

    daLogger.logEvent("Gold Reality Check", gold_results)
    
    val final_results = f"""{"registration_id": ${registration_id}, "passed": ${TestResultsAggregator.passed}, "percentage": ${TestResultsAggregator.percentage}, "actPoints": ${TestResultsAggregator.score}, "maxPoints": ${TestResultsAggregator.maxScore}}"""
    
    daLogger.logEvent("Enb Capstone Final Result", final_results)

    if (display == true) gold_tests.displayResults()

  }
  
  finally{
    for (s <- spark.streams.active) {
      if (s.name == "gold_test") {
        try {
          s.stop()
        }
        catch {
          case e : Throwable =>
            println("!!", e)
        }
      }
    }
  }
}
  
displayHTML("""
Declared the following function:
  <li><span style="color:green; font-weight:bold">realityCheckGold</span></li>
""")

