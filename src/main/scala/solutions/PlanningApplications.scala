package solutions

import java.nio.file.Files
import java.nio.file.Paths
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.ArrayList

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType

class PlanningApplications(val sqlContextParam: SQLContext) {

  private val sqlContext = sqlContextParam
  private var planningApplicationsFile: DataFrame = _
  
  def readData(path: String) {
    
    planningApplicationsFile = sqlContext.read.json(path)
    planningApplicationsFile.registerTempTable("applications")
  }
  
  def saveToFile(path: String, content: ArrayList[String]) {
    Files.write(Paths.get(path), content);
  }
  
  def saveToFile(path: String, content: Array[Byte]) {
    Files.write(Paths.get(path), content);
  }
  
  def discoverSchemaAndSaveToFile(path: String) : StructType = {
    val schema = planningApplicationsFile.schema
    Files.write(Paths.get(path), schema.treeString.getBytes)
    
    schema
  }
  
  def getNumberOfRecords() : Long = {
    planningApplicationsFile.count()
  }
  
  def getUniqueCaseOfficers() : ArrayList[String] = {
    
    val caseOfficers = planningApplicationsFile.select("CASEOFFICER").distinct()
    // save dataFrame directly
    //    caseOfficers.write.mode(SaveMode.Overwrite).save("src/main/resources/data/caseOfficers.txt")

    // save rdd directly
    //    val caseOfficersFile = new File("src/main/resources/data/caseOfficers.txt")
    //    if (caseOfficersFile.exists()) {
    //      caseOfficersFile.delete()
    //    }
    //    caseOfficers.rdd.saveAsTextFile(caseOfficersFile.getPath)

    // save the list from the dataframe
    var caseOfficersList = new ArrayList[String]
    caseOfficers.collect().filter { row => row.getString(0) != "" }.foreach { row => caseOfficersList.add(row.getString(0)) }
    
    caseOfficersList
  }
  
  def getTopAgents(numberOfAgents: Int) : ArrayList[String] = {
    
    val topAgents = sqlContext.sql("SELECT AGENT, COUNT(AGENT) AS agent_occurence FROM applications GROUP BY AGENT ORDER BY agent_occurence DESC").limit(numberOfAgents)
    var topAgentsList = new ArrayList[String]
    topAgents.collect().filter { row => row.getString(0) != "" }.foreach { row => topAgentsList.add(row.getString(0)) }
    
    topAgentsList
  }
  
  def getCasetextWordCount : ArrayList[String] = {
    
    val caseTexts = planningApplicationsFile.select("CASETEXT")
    val caseTextsWords = caseTexts.flatMap { row => row.getString(0).toLowerCase().split("[\\s.,\"()';:/]+") }
    val caseTextsWordsAndCounts = caseTextsWords.map { word => (word, 1) }.reduceByKey(_ + _)
    var caseTextsWordsAndCountsList = new ArrayList[String]
    caseTextsWordsAndCounts.collect().foreach { entry => caseTextsWordsAndCountsList.add(entry._1 + " " + entry._2) }
    
    caseTextsWordsAndCountsList
  }
  
  def getAveragePublicConsultationDurationDays : Long = {
    
    val endDatesStrings = planningApplicationsFile.select( "PUBLICCONSULTATIONENDDATE" )
    val startDatesStrings = planningApplicationsFile.select( "PUBLICCONSULTATIONSTARTDATE" )
    
     val durations = endDatesStrings.rdd.zip(startDatesStrings.rdd)
      .filter(row => { (row._1.getString(0) != "") && (row._2.getString(0) != "") })
      .map(row => {
        val endDate = LocalDate.parse(row._1.getString(0), DateTimeFormatter.ofPattern("dd/MM/yyyy"))
        val startDate = LocalDate.parse(row._2.getString(0), DateTimeFormatter.ofPattern("dd/MM/yyyy"))
        ChronoUnit.DAYS.between(startDate, endDate) 
      })
    
    val sum = durations.fold(0L)((sum, el) => sum + el)
    val lengthMoreData = durations.fold(0L)((avg, el) => avg + 1)
    val average = sum / durations.collect().length
    
    average
  }
  
}