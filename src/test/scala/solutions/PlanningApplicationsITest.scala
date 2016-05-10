package solutions

import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConversions._
import PlanningApplicationITest._
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object PlanningApplicationITest {
  val ReadPath = "src/test/resources/data/planningApplications.json"
  val SchemaPath = "src/test/resources/data/schema.txt"
}

class PlanningApplicationITest extends FunSuite with BeforeAndAfterAll {

  var planningApplications: PlanningApplications = _

  val config = new SparkConf().setAppName("Planning Applications").setMaster("local[*]")
  val sparkContext = new SparkContext(config)
  val sqlContext = new SQLContext(sparkContext)

  override def beforeAll() {
    planningApplications = new PlanningApplications(sqlContext)
    planningApplications.readData(ReadPath)
  }

  test("Discover schema and send to file") {
    val schema = planningApplications.discoverSchemaAndSaveToFile(SchemaPath)
    schema.contains(StructField("AGENTADDRESS", StringType, true))

    assert(schema.contains(StructField("AGENTADDRESS", StringType, true)))
    assert(schema.length == 27)
  }

  test("Get Number Of Records") {
    assert(planningApplications.getNumberOfRecords() == 10482)
  }

  test("Get unique case officers") {
    val caseOfficers = planningApplications.getUniqueCaseOfficers()
    val caseOfficersArray = caseOfficers.toArray()

    assert(caseOfficersArray.distinct.length == caseOfficersArray.length)
    assert(caseOfficersArray(0) == "Mr Neil Armstrong")
    assert(caseOfficersArray(20) == "Mrs Julie Gilmour")
  }

  test("Get top n agents") {
    val n = 20
    val topAgents = planningApplications.getTopAgents(n)

    assert(topAgents(0) == "Michael Rathbone")
    assert(topAgents(7) == "Mr John Harding")
  }

  test("Get Casetext Word Count") {
    val wordsAndCount = planningApplications.getCasetextWordCount
    val wordsAndCountArray = wordsAndCount.toArray()

    assert(wordsAndCountArray.distinct.length == wordsAndCountArray.length)
    assert(wordsAndCountArray(0) == "00712 1")
    assert(wordsAndCountArray(75) == "appearance 45")
  }

  test("Get average public consultation duration in days") {
    val averageDays = planningApplications.getAveragePublicConsultationDurationDays
    assert(averageDays == 55)
  }

  override def afterAll() {
    sparkContext.stop()
  }
}