# Databricks notebook source
import builtins as BI

# Setup the capstone
import re, uuid
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import col, to_date, weekofyear
from pyspark.sql import DataFrame

static_tests = None
bronze_tests = None
silver_tests = None
gold_tests = None 
registration_id = None
final_passed = False

course_name = "Core Partner Enablement"
username = spark.sql("SELECT current_user()").first()[0]
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
user_db = f"dbacademy_{clean_username}_dev_ess_cap"
working_dir = f"dbfs:/user/{username}/dbacademy/dev-ess-cap"

outputPathBronzeTest = f"{working_dir}/bronze_test"
outputPathSilverTest = f"{working_dir}/silver_test"
outputPathGoldTest =   f"{working_dir}/gold_test"

source_path = f"wasbs://courseware@dbacademy.blob.core.windows.net/developer-essentials-capstone/v01"

eventSchema = ( StructType()
  .add('eventName', StringType()) 
  .add('eventParams', StructType() 
    .add('game_keyword', StringType()) 
    .add('app_name', StringType()) 
    .add('scoreAdjustment', IntegerType()) 
    .add('platform', StringType()) 
    .add('app_version', StringType()) 
    .add('device_id', StringType()) 
    .add('client_event_time', TimestampType()) 
    .add('amount', DoubleType()) 
  )     
)

class Key:
  singleStreamDF = (spark
    .readStream
    .schema(eventSchema) 
    .option('streamName','mobilestreaming_test') 
    .option("maxFilesPerTrigger", 1)
    .json(f"{source_path}/solutions/single") 
  )
  bronzeDF  =       spark.read.format("delta").load(f"{source_path}/solutions/bronze") 
  correctLookupDF = spark.read.format("delta").load(f"{source_path}/solutions/lookup")
  silverDF =        spark.read.format("delta").load(f"{source_path}/solutions/silver")
  goldDF =          spark.read.format("delta").load(f"{source_path}/solutions/gold") 

print(f"Declared the following variables:")
print(f" * user_db:     {user_db}")
print(f" * working_dir: {working_dir}")
print()
print(f"Declared the following function:")
print(f" * realityCheckBronze(..)")
print(f" * realityCheckStatic(..)")
print(f" * realityCheckSilver(..)")
print(f" * realityCheckGold(..)")
print(f" * realityCheckFinal()")

# COMMAND ----------

def path_exists(path):
  try:
    return len(dbutils.fs.ls(path)) >= 0
  except Exception:
    return False
  
def install_exercise_datasets(reinstall):
  global registration_id
  min_time = "1 minute"
  max_time = "5 minutes"
  
  existing = path_exists(f"{working_dir}/lookup_data") and path_exists(f"{working_dir}/event_source")
  
  if not reinstall and existing:
    print(f"Skipping install of existing datasets to\n{working_dir}/lookup_data and\n{working_dir}/event_source")
    registration_id = spark.read.json(f"{working_dir}/_meta/config.json").first()["registration_id"]
    return 
  
  # Remove old versions of the previously installed datasets
  if existing:
    print(f"Removing previously installed datasets from\n{working_dir}/lookup_data and\n{working_dir}/event_source\n")
    dbutils.fs.rm(f"{working_dir}/lookup_data", True)
    dbutils.fs.rm(f"{source_path}/event_source", True)
    
  print(f"""Installing the datasets to\n{working_dir}/lookup_data\n{working_dir}/event_source""")
  
  print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the
      region that your workspace is in, this operation can take as little as {min_time} and 
      upwards to {max_time}, but this is a one-time operation.""")

  dbutils.fs.cp(f"{source_path}/lookup_data", f"{working_dir}/lookup_data", True)

  dbutils.fs.cp(f"{source_path}/event_source/part-00000-tid-6718866119967790308-cef1b03e-5fda-4259-885e-e992ca3996c3-25700-c000.json", 
                f"{working_dir}/event_source/file-0.json")
  dbutils.fs.cp(f"{source_path}/event_source/part-00001-tid-6718866119967790308-cef1b03e-5fda-4259-885e-e992ca3996c3-25701-c000.json", 
                f"{working_dir}/event_source/file-1.json")
  dbutils.fs.cp(f"{source_path}/event_source/part-00002-tid-6718866119967790308-cef1b03e-5fda-4259-885e-e992ca3996c3-25702-c000.json", 
                f"{working_dir}/event_source/file-2.json")

  registration_id = str(uuid.uuid4()).replace("-","")
  payload = f"""\u007b"registration_id": "{registration_id}"\u007d\n"""
  dbutils.fs.put(f"{working_dir}/_meta/config.json", payload, overwrite=True)
  
  print(f"""\nThe install of the datasets completed successfully.""")  

try: reinstall = dbutils.widgets.get("reinstall").lower() == "true"
except: reinstall = False
install_exercise_datasets(reinstall)

print(f"\nYour Registration ID is {registration_id}")

# COMMAND ----------

# Setup Bronze
from pyspark.sql import DataFrame
import time

def realityCheckBronze(writeToBronze):
  global bronze_tests
  bronze_tests = TestSuite()
  
  dbutils.fs.rm(outputPathBronzeTest, True)
  dbutils.fs.rm(f"{outputPathBronzeTest}_checkpoint", True)
  
  try:
    writeToBronze(Key.singleStreamDF, outputPathBronzeTest, "bronze_test")

    def groupAndCount(df: DataFrame):
      return df.select('eventName').groupBy('eventName').count()

    for s in spark.streams.active:
        if s.name == "bronze_test":
          first = True
          while (len(s.recentProgress) == 0): 
            if first:
              print("waiting for stream to start...")
              first = False
            time.sleep(5)

    try:
      testDF = (spark
          .read
          .format("delta")
          .load(outputPathBronzeTest))
    except Exception as e:
      print(e)
      testDF = (spark
        .read
        .load(outputPathBronzeTest))

    test_dtype = findColumnDatatype(testDF, 'eventDate')

    historyDF = spark.sql("DESCRIBE HISTORY delta.`{}`".format(outputPathBronzeTest))

    bronze_tests.test(id = "rc_bronze_delta_format", points = 2, description = "Is in Delta format", 
        testFunction = lambda: isDelta(outputPathBronzeTest))
    bronze_tests.test(id = "rc_bronze_contains_columns", points = 2, description = "Dataframe contains eventDate column",              
            testFunction = lambda: verifyColumnsExists(testDF, ['eventDate']))
    bronze_tests.test(id = "rc_bronze_correct_schema", points = 2, description = "Returns correct schema", 
            testFunction = lambda: checkSchema(testDF.schema, Key.bronzeDF.schema))
    bronze_tests.test(id = "rc_bronze_column_check", points = 2, description = "eventDate column is correct data type",              
            testFunction = lambda: test_dtype == "date")
    bronze_tests.test(id = "rc_bronze_null_check", points = 2, description = "Does not contain nulls",              
            testFunction = lambda: checkForNulls(testDF, 'eventParams'))
    bronze_tests.test(id = "rc_bronze_is_streaming", points = 2, description = "Is streaming DataFrame",              
            testFunction = lambda: isStreamingDataframe(historyDF))
    bronze_tests.test(id = "rc_bronze_output_mode", points = 2, description = "Output mode is Append",              
            testFunction = lambda: checkOutputMode(historyDF, "Append"))
    bronze_tests.test(id = "rc_bronze_correct_rows", points = 2, description = "Returns a Dataframe with the correct number of rows",      
            testFunction = lambda: testDF.count() == Key.bronzeDF.count())
    bronze_tests.test(id = "rc_bronze_correct_df", points = 2, description = "Returns the correct Dataframe",             
            testFunction = lambda: compareDataFrames(groupAndCount(testDF), groupAndCount(Key.bronzeDF)))

    daLogger.logTestSuite("Bronze Reality Check", registration_id, bronze_tests)
    bronze_tests.displayResults()

  finally:
    for s in spark.streams.active:
      if s.name == 'bronze_test':
        try:
          s.stop()
        except Exception as e:
          print('!!', e)

None

# COMMAND ----------

# Setup Static
def realityCheckStatic(loadStaticData):
  global static_tests
  static_tests = TestSuite()
  
  testDF = loadStaticData(f"{source_path}/solutions/lookup")
  
  static_tests.test(id = "rc_static_count", points = 2, description = "Has the correct number of rows", 
             testFunction = lambda: testDF.count() == 475)
  static_tests.test(id = "rc_static_schema", points = 2, description = "Returns correct schema", 
               testFunction = lambda: checkSchema(testDF.schema, Key.correctLookupDF.schema))
  
  daLogger.logTestSuite("Static Reality Check", registration_id, static_tests)
  static_tests.displayResults()
  
None

# COMMAND ----------

# Setup Silver
def realityCheckSilver(bronzeToSilver):
  global silver_tests
  silver_tests = TestSuite()

  dbutils.fs.rm(outputPathSilverTest, True)
  dbutils.fs.rm(f"{outputPathSilverTest}_checkpoint", True)
  
  try:

    bronzeToSilver(outputPathBronzeTest, outputPathSilverTest, "silver_test", Key.correctLookupDF)
    
    def groupAndCount(df: DataFrame):
      try:
        return df.select('deviceType').groupBy('deviceType').count()
      except:
        print("deviceType not found")
    
    for s in spark.streams.active:
        first = True
        while (len(s.recentProgress) == 0): 
          if first:
            print("waiting for stream to start...")
            first = False
          time.sleep(5)

    try:
      testDF = (spark
          .read
          .format("delta")
          .load(outputPathSilverTest))
    except Exception as e:
      testDF = (spark
        .read
        .load(outputPathSilverTest))

    historyDF = spark.sql("DESCRIBE HISTORY delta.`{}`".format(outputPathSilverTest))
    
    silver_tests.test(id = "rc_silver_delta_format", points = 2, description = "Is in Delta format", 
            testFunction = lambda: isDelta(outputPathSilverTest))
    silver_tests.test(id = "rc_silver_contains_columns", points = 2, description = "Dataframe contains device_id, client_event_time, deviceType columns",              
            testFunction = lambda: verifyColumnsExists(testDF, ["device_id", "client_event_time", "deviceType"]))
    silver_tests.test(id = "rc_silver_correct_schema", points = 2, description = "Returns correct schema", 
            testFunction = lambda: checkSchema(testDF.schema, Key.silverDF.schema))
    silver_tests.test(id = "rc_silver_null_check", points = 2, description = "Does not contain nulls",              
            testFunction = lambda: checkForNulls(testDF, "eventName"))
    silver_tests.test(id = "rc_silver_is_streaming", points = 2, description = "Is streaming DataFrame",              
            testFunction = lambda: isStreamingDataframe(historyDF))
    silver_tests.test(id = "rc_silver_output_mode", points = 2, description = "Output mode is Append",              
            testFunction = lambda: checkOutputMode(historyDF, "Append"))
    silver_tests.test(id = "rc_silver_correct_rows", points = 2, description = "Returns a Dataframe with the correct number of rows",              
            testFunction = lambda: testDF.count() == Key.silverDF.count())
    silver_tests.test(id = "rc_silver_correct_df", points = 2, description = "Returns the correct Dataframe",              
            testFunction = lambda: compareDataFrames(groupAndCount(testDF), groupAndCount(Key.silverDF)))

    daLogger.logTestSuite("Silver Reality Check", registration_id, silver_tests)
    silver_tests.displayResults()

  finally:
    for s in spark.streams.active:
      if s.name == 'silver_test':
        s.stop()
  
None

# COMMAND ----------

# Setup Gold
def realityCheckGold(silverToGold):
  global gold_tests
  gold_tests = TestSuite()

  dbutils.fs.rm(outputPathGoldTest, True)
  dbutils.fs.rm(f"{outputPathGoldTest}_checkpoint", True)
  
  try:
  
    silverToGold(outputPathSilverTest, outputPathGoldTest, "gold_test")

    for s in spark.streams.active:
        first = True
        while (len(s.recentProgress) == 0): 
          if first:
            print("waiting for stream to start...")
            first = False
          time.sleep(5)
          
    try:
      testDF = (spark
        .read
        .format("delta")
        .load(outputPathGoldTest))
    except Exception as e:
      testDF = (spark
        .read
        .load(outputPathGoldTest))

    historyDF = spark.sql("DESCRIBE HISTORY delta.`{}`".format(outputPathGoldTest))

    gold_tests.test(id = "rc_gold_delta_format", points = 2, description = "Is in Delta format", 
             testFunction = lambda: isDelta(outputPathGoldTest))
    gold_tests.test(id = "rc_gold_contains_columns", points = 2, description = "Dataframe contains week and WAU columns",              
             testFunction = lambda: verifyColumnsExists(testDF, ["week", "WAU"]))
    gold_tests.test(id = "rc_gold_correct_schema", points = 2, description = "Returns correct schema", 
             testFunction = lambda: checkSchema(testDF.schema, Key.goldDF.schema))
    gold_tests.test(id = "rc_gold_null_check", points = 2, description = "Does not contain nulls",              
             testFunction = lambda: checkForNulls(testDF, "eventName"))
    gold_tests.test(id = "rc_gold_is_streaming", points = 2, description = "Is streaming DataFrame",              
             testFunction = lambda: isStreamingDataframe(historyDF))
    gold_tests.test(id = "rc_gold_output_mode", points = 2, description = "Output mode is Complete",              
             testFunction = lambda: checkOutputMode(historyDF, "Complete"))
    gold_tests.test(id = "rc_gold_correct_rows", points = 2, description = "Returns a Dataframe with the correct number of rows",              
             testFunction = lambda: testDF.count() == Key.goldDF.count())
    gold_tests.test(id = "rc_gold_correct_df", points = 2, description = "Returns the correct Dataframe",              
             testFunction = lambda: compareDataFrames(testDF.sort("week"), Key.goldDF.sort("week")))
    
    daLogger.logTestSuite("Gold Reality Check", registration_id, gold_tests)
    gold_tests.displayResults()

  finally:
    for s in spark.streams.active:
      if s.name == 'gold_test':
        s.stop()
  
None

# COMMAND ----------

html_passed = f"""
<html>
<body>
<h2>Congratulations! You're all done!</h2>

While the preliminary evaluation of your project indicates that you have passed, we have a few more validation steps to run on the back-end:<br/>
<ul style="margin:0">
  <li> Code & statistical analysis of your capstone project</li>
  <li> Correlation of your account in our LMS via your email address, <b>{username}</b></li>
  <li> Final preparation of your badge
</ul>


<p>Assuming there are no issues with our last few steps, you will receive your <b>Databricks Developer Essentials Badge</b> within 2 weeks. Notification will be made by email to <b>{username}</b> regarding the availability of your digital badge via <b>Accredible</b>. 
Should we have any issues, such as not finding your email address in our LMS, we will do our best to resolve the issue using the email address provided here.
</p>

<p>Your digital badge will be available in a secure, verifiable, and digital format that you can easily retrieve via <b>Accredible</b>. You can then share your achievement via any number of different social media platforms.</p>

<p>If you have questions about the status of your badge after the initial two-week window, or if the email address listed above is incorrect, please <a href="https://help.databricks.com/s/contact-us?ReqType=training" target="_blank">submit a ticket</a> with the subject "Core Capstone" and your Registration ID (<b>{registration_id}</b>) in the message body. Please allow us 3-5 business days to respond.</p>

One final note: In order to comply with <a href="https://oag.ca.gov/privacy/ccpa" target="_blank">CCPA</a> and <a href="https://gdpr.eu/" target="_blank">GDPR</a>, which regulate the collection of your personal information, the status of this capstone and its correlation to your email address will be deleted within 30 days of its submission.
</body>
</html>
"""

html_failed = f"""
<html>
<body>
<h2>Almost There!</h2>

<p>Our preliminary evaluation of your project indicates that you have not passed.</p>

<p>In order for your project to be submitted <b>all</b> reality checks must pass.</p>

<p>In some cases this problem can be resolved by simply clearning the notebook's state (<b>Clear State & Results</b>) and then selecting <b>Run All</b> from the toolbar above.</p>

<p>If your project continues to fail validation, please review each step above to ensure that you are have properly addressed all the corresponding requirements.</p>
</body>
</html>
"""

# Setup Final
def realityCheckFinal():
    global final_passed
    
    suite = TestSuite()
    suite.testEquals(f"final.static-passed", "Reality Check Bronze passed", static_tests.passed, True)
    suite.testEquals(f"final.bronze-passed", "Reality Check Static passed", bronze_tests.passed, True)
    suite.testEquals(f"final.silver-passed", "Reality Check Silver passed", silver_tests.passed, True)
    suite.testEquals(f"final.final-passed",  "Reality Check Gold passed",   gold_tests.passed, True)
  
    final_passed = suite.passed
    daLogger.logTestSuite("Final Reality Check", registration_id, suite)
    daLogger.logAggregation("Capstone", registration_id, TestResultsAggregator)

    suite.displayResults()

    if final_passed and TestResultsAggregator.passed:
      displayHTML(html_passed)
      daLogger.logCompletion(registration_id, username)
    else:
      displayHTML(html_failed)

None

# COMMAND ----------

class CapstoneLogger:
  
  def logTestResult(self, event_id, registration_id, result):
    self.logEvent(event_id =        event_id, 
                  registration_id = registration_id, 
                  description =     result.test.description, 
                  passed =          result.passed, 
                  points =          result.points, 
                  max_points =      result.test.points)
  
  def logTestSuite(self, event_id, registration_id, suite):
    self.logEvent(event_id =        event_id, 
                  registration_id = registration_id, 
                  description =     None, 
                  passed =          suite.passed, 
                  points =          suite.score, 
                  max_points =      suite.maxScore)
  
  def logAggregation(self, event_id, registration_id, aggregate):
    self.logEvent(event_id =        event_id, 
                  registration_id = registration_id, 
                  description =     None, 
                  passed =          aggregate.passed, 
                  points =          aggregate.score, 
                  max_points =      aggregate.maxScore)
  
  def logCompletion(self, registration_id:str, email_address:str):
    import time, json, requests
    try:
      content = {
        "registration_id": registration_id, 
        "email_address":   email_address, 
      }
      try:
        response = requests.put( 
            url="https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod/capstone/completed", 
            json=content,
            headers={
              "Accept": "application/json; charset=utf-8",
              "Content-Type": "application/json; charset=utf-8"
            })
        assert response.status_code == 200, f"Expected HTTP response code 200, found {response.status_code}"
        
      except requests.exceptions.RequestException as e:
        raise Exception("Exception sending message") from e
      
    except Exception as e:
      raise Exception("Exception constructing message") from e

  def logEvent(self, event_id:str, registration_id:str, description:str, passed:str, points:int, max_points:int):
    import time, json, requests
    try:
      content = {
        "module_name":     "essentials-capstone-v2",
        "lesson_name":     "Capstone",
        "language":        "python",

        "event_id":        event_id,
        "event_time":      f"{BI.int(BI.round((time.time() * 1000)))}",
        
        "registration_id": registration_id, 
        "description":     description, 
        "passed":          passed, 
        "points":          points, 
        "max_points":      max_points,
      }
      try:
        response = requests.post( 
            url="https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod/capstone/status", 
            json=content,
            headers={
              "Accept": "application/json; charset=utf-8",
              "Content-Type": "application/json; charset=utf-8"
            })
        assert response.status_code == 200, f"Expected HTTP response code 200, found {response.status_code}"
        
      except requests.exceptions.RequestException as e:
        raise Exception("Exception sending message") from e
      
    except Exception as e:
      raise Exception("Exception constructing message") from e
    
daLogger = CapstoneLogger()

None

# COMMAND ----------

# These imports are OK to provide for students
import pyspark
from typing import Callable, Any, Iterable, List, Set, Tuple
import uuid


#############################################
# Test Suite classes
#############################################

# Test case
class TestCase(object):
  __slots__=('description', 'testFunction', 'id', 'uniqueId', 'dependsOn', 'escapeHTML', 'points')
  def __init__(self,
               description:str,
               testFunction:Callable[[], Any],
               id:str=None,
               dependsOn:Iterable[str]=[],
               escapeHTML:bool=False,
               points:int=1):
    
    self.description=description
    self.testFunction=testFunction
    self.id=id
    self.dependsOn=dependsOn
    self.escapeHTML=escapeHTML
    self.points=points

# Test result
class TestResult(object):
  __slots__ = ('test', 'skipped', 'debug', 'passed', 'status', 'points', 'exception', 'message')
  def __init__(self, test, skipped = False, debug = False):
    try:
      self.test = test
      self.skipped = skipped
      self.debug = debug
      if skipped:
        self.status = 'skipped'
        self.passed = False
        self.points = 0
      else:
        assert test.testFunction() != False, "Test returned false"
        self.status = "passed"
        self.passed = True
        self.points = self.test.points
      self.exception = None
      self.message = ""
    except Exception as e:
      self.status = "failed"
      self.passed = False
      self.points = 0
      self.exception = e
      self.message = repr(self.exception)
      if (debug and not isinstance(e, AssertionError)):
        raise e

# Decorator to lazy evaluate - used by TestSuite
def lazy_property(fn):
    '''Decorator that makes a property lazy-evaluated.
    '''
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazy_property

  
testResultsStyle = """
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
    """.strip()

# Test suite class
class TestSuite(object):
  def __init__(self) -> None:
    self.ids = set()
    self.testCases = list()

  @lazy_property
  def testResults(self) -> List[TestResult]:
    return self.runTests()
  
  def runTests(self, debug=False) -> List[TestResult]:
    import re
    failedTests = set()
    testResults = list()

    for test in self.testCases:
      skip = any(testId in failedTests for testId in test.dependsOn)
      result = TestResult(test, skip, debug)

      if (not result.passed and test.id != None):
        failedTests.add(test.id)

      if result.test.id: event_id = "Test-"+result.test.id 
      elif result.test.description: event_id = "Test-"+re.sub("[^a-zA-Z0-9_]", "", result.test.description)
      else: event_id = "Test-"+str(uuid.uuid1())

      daLogger.logTestResult(event_id, registration_id, result)
      
      testResults.append(result)
      TestResultsAggregator.update(result)
    
    return testResults

  def _display(self, cssClass:str="results", debug=False) -> None:
    from html import escape
    testResults = self.testResults if not debug else self.runTests(debug=True)
    lines = []
    lines.append(testResultsStyle)
    lines.append("<table class='"+cssClass+"'>")
    lines.append("  <tr><th class='points'>Points</th><th class='test'>Test</th><th class='result'>Result</th></tr>")
    for result in testResults:
      resultHTML = "<td class='result "+result.status+"'><span class='message'>"+result.message+"</span></td>"
      descriptionHTML = escape(str(result.test.description)) if (result.test.escapeHTML) else str(result.test.description)
      lines.append("  <tr><td class='points'>"+str(result.points)+"</td><td class='test'>"+descriptionHTML+"</td>"+resultHTML+"</tr>")
    lines.append("  <caption class='points'>Score: "+str(self.score)+"</caption>")
    lines.append("</table>")
    html = "\n".join(lines)
    displayHTML(html)
  
  def displayResults(self) -> None:
    self._display("results")
  
  def grade(self) -> int:
    self._display("grade")
    return self.score
  
  def debug(self) -> None:
    self._display("grade", debug=True)
  
  @lazy_property
  def score(self) -> int:
    return __builtins__.sum(map(lambda result: result.points, self.testResults))
  
  @lazy_property
  def maxScore(self) -> int:
    return __builtins__.sum(map(lambda result: result.test.points, self.testResults))

  @lazy_property
  def percentage(self) -> int:
    return 0 if self.maxScore == 0 else int(100.0 * self.score / self.maxScore)

  @lazy_property
  def passed(self) -> bool:
    return self.percentage == 100

  def addTest(self, testCase: TestCase):
    if not testCase.id: raise ValueError("The test cases' id must be specified")
    if testCase.id in self.ids: raise ValueError(f"Duplicate test case id: {testCase.id}")
    self.testCases.append(testCase)
    self.ids.add(testCase.id)
    return self
  
  def test(self, id:str, description:str, testFunction:Callable[[], Any], points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)
  
  def testEquals(self, id:str, description:str, valueA, valueB, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: valueA == valueB
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)
    
  def testFloats(self, id:str, description:str, valueA, valueB, tolerance=0.01, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: compareFloats(valueA, valueB, tolerance)
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)

  def testRows(self, id:str, description:str, rowA: pyspark.sql.Row, rowB: pyspark.sql.Row, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: compareRows(rowA, rowB)
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)
  
  def testDataFrames(self, id:str, description:str, dfA: pyspark.sql.DataFrame, dfB: pyspark.sql.DataFrame, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: compareDataFrames(dfA, dfB)
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)
  
  def testContains(self, id:str, description:str, listOfValues, value, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
    testFunction = lambda: value in listOfValues
    testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
    return self.addTest(testCase)
  
class __TestResultsAggregator(object):
  testResults = dict()
  
  def update(self, result:TestResult):
    self.testResults[result.test.id] = result
    return result
  
  @lazy_property
  def score(self) -> int:
    return __builtins__.sum(map(lambda result: result.points, self.testResults.values()))
  
  @lazy_property
  def maxScore(self) -> int:
    return __builtins__.sum(map(lambda result: result.test.points, self.testResults.values()))

  @lazy_property
  def percentage(self) -> int:
    return 0 if self.maxScore == 0 else int(100.0 * self.score / self.maxScore)
  
  @lazy_property
  def passed(self) -> bool:
    return self.percentage == 100

  def displayResults(self):
    displayHTML(testResultsStyle + f"""
    <table class='results'>
      <tr><th colspan="2">Test Summary</th></tr>
      <tr><td>Number of Passing Tests</td><td style="text-align:right">{self.score}</td></tr>
      <tr><td>Number of Failing Tests</td><td style="text-align:right">{self.maxScore-self.score}</td></tr>
      <tr><td>Percentage Passed</td><td style="text-align:right">{self.percentage}%</td></tr>
    </table>
    """)
# Lazy-man's singleton
TestResultsAggregator = __TestResultsAggregator()  

None

# COMMAND ----------

from pyspark.sql import Row, DataFrame

def returnTrue():
  return True

def compareFloats(valueA, valueB, tolerance=0.01):
  # Usage: compareFloats(valueA, valueB) (uses default tolerance of 0.01)
  #        compareFloats(valueA, valueB, tolerance=0.001)
 
  from builtins import abs 
  try:
    if (valueA == None and valueB == None):
         return True
      
    else:
         return abs(float(valueA) - float(valueB)) <= tolerance 
      
  except:
    return False
  

def compareRows(rowA: Row, rowB: Row):
  # Usage: compareRows(rowA, rowB)
  # compares two Dictionaries
  
  if (rowA == None and rowB == None):
    return True
  
  elif (rowA == None or rowB == None):
    return False
  
  else: 
    return rowA.asDict() == rowB.asDict()


def compareDataFrames(dfA: DataFrame, dfB: DataFrame):
  from functools import reduce
  # Usage: compareDataFrames(dfA, dfB)
    
  if (dfA == None and dfB == None):
    return True
  else:  
    n = dfA.count()
  
    if (n != dfB.count()):
      return False
  
    kv1 = dfA.rdd.zipWithIndex().map(lambda t : (t[1], t[0])).collectAsMap()
    kv2 = dfB.rdd.zipWithIndex().map(lambda t : (t[1], t[0])).collectAsMap()
  
    kv12 = [kv1, kv2]
    d = {}

    for k in kv1.keys():
      d[k] = tuple(d[k] for d in kv12)
  
    return reduce(lambda a, b: a and b, [compareRows(rowTuple[0], rowTuple[1]) for rowTuple in d.values()])

def checkSchema(schemaA, schemaB, keepOrder=True, keepNullable=False): 
  # Usage: checkSchema(schemaA, schemaB, keepOrder=false, keepNullable=false)
  
  from pyspark.sql.types import StructField
  
  if (schemaA == None and schemaB == None):
    return True
  
  elif (schemaA == None or schemaB == None):
    return False
  
  else:
    schA = schemaA
    schB = schemaB

    if (keepNullable == False):  
        schA = [StructField(s.name, s.dataType) for s in schemaA]
        schB = [StructField(s.name, s.dataType) for s in schemaB]
  
    if (keepOrder == True):
      return [schA] == [schB]
    else:
      return set(schA) == set(schB)
  
None

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum
import os

def verifyColumnsExists(df: DataFrame, columnNames):
  return all(col in df.columns for col in columnNames)

def findColumnDatatype(df: DataFrame, columnName):
  try:
    return df.select(columnName).dtypes[0][1]
  except Exception as e:
    return False

def isDelta(path):
  found = False
  for file in dbutils.fs.ls(path): 
    if file.name == "_delta_log/":
      found = True
  return found
  
def checkForNulls(df: DataFrame, columnName):
  try:
    nullCount = df.select(sum(col(columnName).isNull().astype(IntegerType())).alias('nullCount')).collect()[0].nullCount
    if (nullCount > 0):
      return False
  except Exception as e:
    return True
  
def isStreamingDataframe(df: DataFrame):
  return df.take(1)[0].operation == "STREAMING UPDATE"

def checkOutputMode(df: DataFrame, mode):
  return df.take(1)[0].operationParameters['outputMode'] == mode

print("Finished setting up the capstone environment.")

