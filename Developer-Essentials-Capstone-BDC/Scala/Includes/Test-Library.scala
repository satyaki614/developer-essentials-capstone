// Databricks notebook source

class DatabricksAcademyLogger() extends org.apache.spark.scheduler.SparkListener {
  import org.apache.spark.scheduler._

  val hostname = "https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod"

  def logEvent(eventId: String, message: String = null):Unit = {
    import org.apache.http.entity._
    import org.apache.http.impl.client.{HttpClients, BasicResponseHandler}
    import org.apache.http.client.methods.HttpPost
    import java.net.URLEncoder.encode
    import org.json4s.jackson.Serialization
    implicit val formats = org.json4s.DefaultFormats

    val start = System.currentTimeMillis // Start the clock

    var client:org.apache.http.impl.client.CloseableHttpClient = null

    try {
      val utf8 = java.nio.charset.StandardCharsets.UTF_8.toString;
      val username = encode(getUsername(), utf8)
      val moduleName = encode(getModuleName(), utf8)
      val lessonName = encode(getLessonName(), utf8)
      val event = encode(eventId, utf8)
      val url = s"$hostname/logger"
    
      val content = Map(
        "tags" ->       getTags().map(x => (x._1.name, s"${x._2}")),
        "moduleName" -> getModuleName(),
        "lessonName" -> getLessonName(),
        "orgId" ->      getTag("orgId", "unknown"),
//         "username" ->   getUsername(),
        "eventId" ->    eventId,
        "eventTime" ->  s"${System.currentTimeMillis}",
        "language" ->   getTag("notebookLanguage", "unknown"),
        "notebookId" -> getTag("notebookId", "unknown"),
        "sessionId" ->  getTag("sessionId", "unknown"),
        "message" ->    message
      )
      
      val client = HttpClients.createDefault()
      val httpPost = new HttpPost(url)
      val entity = new StringEntity(Serialization.write(content))      

      httpPost.setEntity(entity)
      httpPost.setHeader("Accept", "application/json")
      httpPost.setHeader("Content-type", "application/json")

      val response = client.execute(httpPost)
      val duration = System.currentTimeMillis - start // Stop the clock
      org.apache.log4j.Logger.getLogger(getClass).info(s"DAL-$eventId-SUCCESS: Event completed in $duration ms")
      
    } catch {
      case e:Exception => {
        val duration = System.currentTimeMillis - start // Stop the clock
        val msg = s"DAL-$eventId-FAILURE: Event completed in $duration ms"
        org.apache.log4j.Logger.getLogger(getClass).error(msg, e)
      }
    } finally {
      if (client != null) {
        try { client.close() } 
        catch { case _:Exception => () }
      }
    }
  }
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = logEvent("JobEnd: " + jobEnd.jobId)
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = logEvent("JobStart: " + jobStart.jobId)
}

// Get the user's username
def getUsername(): String = {
  return try {
    dbutils.widgets.get("databricksUsername")
  } catch {
    case _: Exception => getTag("user", java.util.UUID.randomUUID.toString.replace("-", ""))
  }
}

// Get the user's userhome
def getUserhome(): String = {
  val username = getUsername()
  return s"dbfs:/user/$username"
}

def getModuleName(): String = {
  // This will/should fail if module-name is not defined in the Classroom-Setup notebook
  return "enb-capstone"
}

def getLessonName(): String = {
  // If not specified, use the notebook's name.
  return dbutils.notebook.getContext.notebookPath.get.split("/").last
}

def getWorkingDir(courseType:String): String = {
  val langType = "s" // for scala
  val moduleName = getModuleName().replaceAll("[^a-zA-Z0-9]", "_").toLowerCase()
  val lessonName = getLessonName().replaceAll("[^a-zA-Z0-9]", "_").toLowerCase()
  return "%s/%s/%s_%s%s".format(getUserhome(), moduleName, lessonName, langType, courseType)
}

def getTags(): Map[com.databricks.logging.TagDefinition,String] = {
  com.databricks.logging.AttributionContext.current.tags
} 

// Get a single tag's value
def getTag(tagName: String, defaultValue: String = null): String = {
  val values = getTags().collect({ case (t, v) if t.name == tagName => v }).toSeq
  values.size match {
    case 0 => defaultValue
    case _ => values.head.toString
  }
}

val daLogger = new DatabricksAcademyLogger()

display(Seq())

// COMMAND ----------

def compareFloats(valueA: Any, valueB: Any, tolerance: Double=0.01): Boolean = {
  // Usage: compareFloats(valueA, valueB) (uses default tolerance of 0.01)
  //        compareFloats(valueA, valueB, tolerance=0.001)
  
  import scala.math
  try{
       if (valueA == null && valueB == null) {
         true
       } else if (valueA == null || valueB == null) {
         false
       } else {
         math.abs(valueA.asInstanceOf[Number].doubleValue - valueB.asInstanceOf[Number].doubleValue) <= tolerance 
       }
  } catch {
    case e: ClassCastException => {
      false
   }   
  }
}

def compareRows(rowA: org.apache.spark.sql.Row, rowB: org.apache.spark.sql.Row): Boolean = {
  // Usage: compareRows(rowA, rowB)
  // compares two rows as unordered Maps

  if (rowA == null && rowB == null) {
    true
    
  } else if (rowA == null || rowB == null) {
    false
    
  // for some reason, the schema didn't make it
  } else if (rowA.schema == null || rowB.schema == null) {
    rowA.toSeq.toSet == rowB.toSeq.toSet
    
  } else {
    rowA.getValuesMap[String](rowA.schema.fieldNames.toSeq) == rowB.getValuesMap[String](rowB.schema.fieldNames.toSeq)
  }
}

def compareDataFrames(dfA: org.apache.spark.sql.DataFrame, dfB: org.apache.spark.sql.DataFrame): Boolean = {
  // Usage: compareDataFrames(dfA, dfB)
  //        rows have to be ordered
  
  if (dfA == null && dfB == null) {
    true
    
  } else if (dfA == null || dfB == null) {
    false
    
  } else { 
  
    // if the sizes are different, the DFs aren't equal  
    if (dfA.count != dfB.count) {
      false
    }

    // These appropaches didn't work (well)
  
    // dfA.exceptAll(dfB).isEmpty && dfB.exceptAll(dfA).isEmpty
    // sloooooow and doesn't look at row order
  
    // (dfA.rdd zip dfB.rdd)
    // doesn't always work because rows can be in different partitions
  
    val kv1 = dfA.rdd.zipWithIndex().map { case (v, i) => i -> v }
    val kv2 = dfB.rdd.zipWithIndex().map { case (v, i) => i -> v }
    val kv12= kv1.leftOuterJoin(kv2).map {
      case (i, (v1, v2)) => i -> (v1, v2.get)
    }

    kv12.sortByKey().values.map(r => compareRows(r._1, r._2)).reduce(_ && _) 
  }
}

def checkSchema(schemaA: org.apache.spark.sql.types.StructType, schemaB: org.apache.spark.sql.types.StructType, keepOrder: Boolean=true, keepNullable: Boolean=false): Boolean = {
  // Usage: checkSchema(schemaA, schemaC, keepOrder=false, keepNullable=false)
  
  if (schemaA == null && schemaB == null) {
    return true
    
  } else if (schemaA == null || schemaB == null) {
    return false
  
  } else {
    var schA = schemaA.toSeq
    var schB = schemaB.toSeq

    if (keepNullable == false) {   
      schA = schemaA.map(_.copy(nullable=true)) 
      schB = schemaB.map(_.copy(nullable=true)) 
    }
  
    if (keepOrder == true) {
      schA == schB
    } else {
      schA.toSet == schB.toSet
    }
  }
}

displayHTML("Initialized assertions framework.")

// COMMAND ----------

//*******************************************
// TEST SUITE CLASSES
//*******************************************

// Test case
case class TestCase(description:String,
                    testFunction:()=>Any,
                    id:String=null,
                    dependsOn:Seq[String]=Nil,
                    escapeHTML:Boolean=false,
                    points:Int=1)

// Test result
case class TestResult(test: TestCase, skipped:Boolean = false, debug:Boolean = false) {
  val exception: Option[Throwable] = {
    if (skipped)
      None
    else if (debug) {
      if (test.testFunction() != false)
        None
      else 
        Some(new AssertionError("Test returned false"))
    } else {
      try {
        assert(test.testFunction() != false, "Test returned false")
        None
      } catch {
        case e: Exception => Some(e)
        case e: AssertionError => Some(e)
      }
    }
  }

  val passed: Boolean = !skipped && exception.isEmpty

  val message: String = {
    exception.map(ex => {
      val msg = ex.getMessage()
      if (msg == null || msg.isEmpty()) ex.toString() else msg
    }).getOrElse("")
  }
  
  val status: String = if (skipped) "skipped" else if (passed) "passed" else "failed"
  
  val points: Int = if (passed) test.points else 0
}

val testResultsStyle = """
<style>
  table { text-align: left; border-collapse: collapse; margin: 1em; caption-side: bottom; font-family: Sans-Serif; font-size: 16px}
  caption { text-align: left; padding: 5px }
  th, td { border: 1px solid #ddd; padding: 5px }
  th { background-color: #ddd }
  .passed { background-color: #97d897 }
  .failed { background-color: #e2716c }
  .skipped { background-color: #f9d275 }
  .results .points { display: none }
  .results .message { display: none }
  .results .passed::before  { content: "Passed" }
  .results .failed::before  { content: "Failed" }
  .results .skipped::before { content: "Skipped" }
  .grade .passed  .message:empty::before { content:"Passed" }
  .grade .failed  .message:empty::before { content:"Failed" }
  .grade .skipped .message:empty::before { content:"Skipped" }
</style>
    """.trim

object TestResultsAggregator {
  val testResults = scala.collection.mutable.Map[String,TestResult]()
  def update(result:TestResult):TestResult = {
    testResults.put(result.test.id, result)
    return result
  }
  def score = testResults.values.map(_.points).sum
  def maxScore = testResults.values.map(_.test.points).sum
  def percentage = (if (maxScore == 0) 0 else 100.0 * score / maxScore).toInt
  def passed = percentage == 100
  def displayResults():Unit = {
    displayHTML(testResultsStyle + s"""
    <table class='results'>
      <tr><th colspan="2">Test Summary</th></tr>
      <tr><td>Number of Passing Tests</td><td style="text-align:right">${score}</td></tr>
      <tr><td>Number of Failing Tests</td><td style="text-align:right">${maxScore-score}</td></tr>
      <tr><td>Percentage Passed</td><td style="text-align:right">${percentage}%</td></tr>
    </table>
    """)
  }
}

// Test Suite
case class TestSuite() {
  
  val testCases : scala.collection.mutable.ArrayBuffer[TestCase] = scala.collection.mutable.ArrayBuffer[TestCase]()
  val ids : scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer[String]()
  
  import scala.collection.mutable.ListBuffer
  import scala.xml.Utility.escape
  
  def runTests(debug:Boolean = false):scala.collection.mutable.ArrayBuffer[TestResult] = {
    val failedTests = scala.collection.mutable.Set[String]()
    val results = scala.collection.mutable.ArrayBuffer[TestResult]()
    for (testCase <- testCases) {
      val skip = testCase.dependsOn.exists(failedTests contains _)
      val result = TestResult(testCase, skip, debug)

      if (result.passed == false && testCase.id != null) {
        failedTests += testCase.id
      }
      val eventId = "Test-" + (if (result.test.id != null) result.test.id 
                          else if (result.test.description != null) result.test.description.replaceAll("[^a-zA-Z0-9_]", "").toUpperCase() 
                          else java.util.UUID.randomUUID)

      val message = f"""{"registration_id": ${registration_id}, "testId": "${eventId}", "description": "${result.test.description}", "status": "${result.status}", "actPoints": ${result.points}, "maxPoints": ${result.test.points}}"""

      daLogger.logEvent(eventId, message)
      
      results += result
      TestResultsAggregator.update(result)
    }
    return results
  }

  lazy val testResults = runTests()

  private def display(cssClass:String="results", debug:Boolean=false) : Unit = {
    val testResults = if (!debug) this.testResults else runTests(debug=true)
    val lines = ListBuffer[String]()
    lines += testResultsStyle
    lines += s"<table class='$cssClass'>"
    lines += "  <tr><th class='points'>Points</th><th class='test'>Test</th><th class='result'>Result</th></tr>"
    for (result <- testResults) {
      val resultHTML = s"<td class='result ${result.status}'><span class='message'>${result.message}</span></td>"
      val descriptionHTML = if (result.test.escapeHTML) escape(result.test.description) else result.test.description
      lines += s"  <tr><td class='points'>${result.points}</td><td class='test'>$descriptionHTML</td>$resultHTML</tr>"
    }
    lines += s"  <caption class='points'>Score: $score</caption>"
    lines += "</table>"
    val html = lines.mkString("\n")
    displayHTML(html)
  }
  
  def displayResults() : Unit = {
    display("results")
  }
  
  def grade() : Int = {
    display("grade")
    score
  }
  
  def debug() : Unit = {
    display("grade", debug=true)
  }
  
  def score = testResults.map(_.points).sum
  
  def maxScore : Integer = testCases.map(_.points).sum

  def percentage = (if (maxScore == 0) 0 else 100.0 * score / maxScore).toInt
  
  def passed = percentage == 100
  
  def addTest(testCase: TestCase):TestSuite = {
    if (testCase.id == null) throw new IllegalArgumentException("The test cases' id must be specified")
    if (ids.contains(testCase.id)) throw new IllegalArgumentException(f"Duplicate test case id: {testCase.id}")
    testCases += testCase
    ids += testCase.id
    return this
  }
  
  def test(id:String, description:String, testFunction:()=>Any, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
    val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return addTest(testCase)
  }
  
  def testEquals(id:String, description:String, valueA:Any, valueB:Any, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
    val testFunction = ()=> (valueA == valueB)
    val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return addTest(testCase)
  }
  
  def testFloats(id:String, description:String, valueA: Any, valueB: Any, tolerance: Double=0.01, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
    val testFunction = ()=> compareFloats(valueA, valueB, tolerance)
    val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return addTest(testCase)
  }
  
  def testRows(id:String, description:String, rowA: org.apache.spark.sql.Row, rowB: org.apache.spark.sql.Row, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
    val testFunction = ()=> compareRows(rowA, rowB)
    val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return addTest(testCase)
  }
  
  def testDataFrames(id:String, description:String, dfA: org.apache.spark.sql.DataFrame, dfB: org.apache.spark.sql.DataFrame, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
    val testFunction = ()=> compareDataFrames(dfA, dfB)
    val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return addTest(testCase)
  }

  def testContains(id:String, description:String, list:Seq[Any], value:Any, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
    val testFunction = ()=> list.contains(value)
    val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return addTest(testCase)
  }
}

displayHTML("Initialized testing framework.")

// COMMAND ----------

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.{IntegerType}

def verifyColumnsExists(df: org.apache.spark.sql.DataFrame, columnNames: List[String]): Boolean = {
  columnNames forall (df.columns contains)
}

def findColumnDatatype(df: org.apache.spark.sql.DataFrame, columnName: String): String = {
  try
  {
    return df.select($"$columnName").dtypes(0)._2
  }
  catch 
  {
    case e : Throwable =>
      return "Column not found in DataFrame"
  } 
}

def isDelta(path: String): Boolean = {
  var found = false
  for (file <- dbutils.fs.ls(path)) {
    if (file.name == "_delta_log/") {
      found = true
    }
  } 
  return found
}

def checkForNulls(df: org.apache.spark.sql.DataFrame, columnName: String): Boolean = {
  var nullCount = df.select(sum($"$columnName".isNull.cast(IntegerType)).alias("nullCount")).collect()(0).getLong(0).toInt
  if (nullCount > 0) return true else return false
}
  
def isStreamingDataframe(df: org.apache.spark.sql.DataFrame): Boolean = {
  return df.take(1)(0)(4) == "STREAMING UPDATE"
}

def checkOutputMode(df: org.apache.spark.sql.DataFrame, mode: String): Boolean = {
  return df.select($"operationParameters").as[Map[String, String]].take(1).head("outputMode") == mode
}

display(Seq())

