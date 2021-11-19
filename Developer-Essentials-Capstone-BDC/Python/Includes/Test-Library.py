# Databricks notebook source

class DatabricksAcademyLogger:
  
  def logEvent(self, eventId: str, message: str = None):
    import time
    import json
    import requests

    hostname = "https://rqbr3jqop0.execute-api.us-west-2.amazonaws.com/prod"
    
    try:
      username = getUsername().encode("utf-8")
      moduleName = getModuleName().encode("utf-8")
      lessonName = getLessonName().encode("utf-8")
      event = eventId.encode("utf-8")
    
      content = {
        "tags":       dict(map(lambda x: (x[0], str(x[1])), getTags().items())),
        "moduleName": getModuleName(),
        "lessonName": getLessonName(),
        "orgId":      getTag("orgId", "unknown"),
#         "username":   getUsername(),
        "eventId":    eventId,
        "eventTime":  f"{int(__builtin__.round((time.time() * 1000)))}",
        "language":   getTag("notebookLanguage", "unknown"),
        "notebookId": getTag("notebookId", "unknown"),
        "sessionId":  getTag("sessionId", "unknown"),
        "message":    message
      }
      
      try:
        response = requests.post( 
            url=f"{hostname}/logger", 
            json=content,
            headers={
              "Accept": "application/json; charset=utf-8",
              "Content-Type": "application/json; charset=utf-8"
            })
      except requests.exceptions.RequestException as e:
        print("REQUEST ERROR ", e)
      
    except Exception as e:
      print("PYTHON ERROR", e)
      pass
    
# Get the user's username
def getUsername() -> str:
  import uuid
  try:
    return dbutils.widgets.get("databricksUsername")
  except:
    return getTag("user", str(uuid.uuid1()).replace("-", ""))

# Get the user's userhome
def getUserhome() -> str:
  username = getUsername()
  return "dbfs:/user/{}".format(username)

def getModuleName() -> str: 
  # This will/should fail if module-name is not defined in the Classroom-Setup notebook
  return "enb-capstone"

def getLessonName() -> str:
  # If not specified, use the notebook's name.
  return dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).split("/")[-1]

def getWorkingDir(courseType:str) -> str:
  import re
  langType = "p" # for python
  moduleName = re.sub(r"[^a-zA-Z0-9]", "_", getModuleName()).lower()
  lessonName = re.sub(r"[^a-zA-Z0-9]", "_", getLessonName()).lower()
  return "{}/{}/{}_{}{}".format(getUserhome(), moduleName, lessonName, langType, courseType)

# Get all tags
def getTags() -> dict: 
  return sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
    dbutils.entry_point.getDbutils().notebook().getContext().tags()
  )

# Get a single tag's value
def getTag(tagName: str, defaultValue: str = None) -> str:
  values = getTags()[tagName]
  try:
    if len(values) > 0:
      return values
  except:
    return defaultValue
  
  
daLogger = DatabricksAcademyLogger()

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

      if result.test.id: eventId = "Test-"+result.test.id 
      elif result.test.description: eventId = "Test-"+re.sub("[^a-zA-Z0-9_]", "", result.test.description).upper()
      else: eventId = "Test-"+str(uuid.uuid1())

      message = f"{{\"registration_id\": {registration_id}, \"testId\": \"{eventId}\", \"description\": \"{result.test.description}\", \"status\": \"{result.status}\", \"actPoints\": {result.points}, \"maxPoints\": {result.test.points}}}"

      daLogger.logEvent(eventId, message)

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

