package solutions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object ApplicationMain {

  private val ReadPath = "src/main/resources/data/planningApplications.json"
  private val SchemaPath = "src/main/resources/data/schema.txt"
  private val NumberOfRecordsPath = "src/main/resources/data/numberOFRecords.txt"
  private val CaseOfficersPath = "src/main/resources/data/caseOfficers.txt"
  private val TopAgentsPath = "src/main/resources/data/topAgents.txt"
  private val CasetextWordCountPath = "src/main/resources/data/wordOccurence.txt"
  private val AverageConsultationDaysPath = "src/main/resources/data/averageConsultationDays.txt"

  def main(args: Array[String]) {

    val config = new SparkConf().setAppName("Planning Applications").setMaster("local[*]")
    val sparkContext = new SparkContext(config)
    val sqlContext = new SQLContext(sparkContext)

    val planningApplications = new PlanningApplications(sqlContext)

    // read data
    planningApplications.readData(ReadPath)

    // 1. Discover the schema of the input dataset and output it to a file.
    planningApplications.discoverSchemaAndSaveToFile(SchemaPath)

    // 2. What is the total number of planning application records in the dataset? Feel free to output this to a file or standard output on the console.
    planningApplications.saveToFile(NumberOfRecordsPath, planningApplications.getNumberOfRecords.toString.getBytes)

    // 3. Identify the set of case officers (CASEOFFICER field) and output a unique list of these to a file.
    planningApplications.saveToFile(CaseOfficersPath, planningApplications.getUniqueCaseOfficers)

    // 4. Who are the top N agents (AGENT field) submitting the most number of applications? Allow N to be configurable and output the list to a file.
    val n = 30
    planningApplications.saveToFile(TopAgentsPath, planningApplications.getTopAgents(n))

    // 5. Count the occurrence of each word within the case text (CASETEXT field) across all planning application records. Output each word and the corresponding count to a file.
    planningApplications.saveToFile(CasetextWordCountPath, planningApplications.getCasetextWordCount)

    // 6. Measure the average public consultation duration in days (i.e. the difference between PUBLICCONSULTATIONENDDATE and PUBLICCONSULTATIONSTARTDATE fields). Feel free to output this to a file or standard output on the console.
    planningApplications.saveToFile(AverageConsultationDaysPath, planningApplications.getAveragePublicConsultationDurationDays.toString.getBytes)

    sparkContext.stop()
  }
}