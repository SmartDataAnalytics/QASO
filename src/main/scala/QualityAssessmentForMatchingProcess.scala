import org.apache.jena.graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*
* Created by Shimaa Ibrahim 10 February 2022
*/ class QualityAssessmentForMatchingProcess(sparkSession: SparkSession) {
  val ontoStat = new OntologyStatistics(sparkSession)
  val matchStat = new MatchingStatistics(sparkSession)
  println("Please add the class and property alignments (matching results) separately to the matching result directory. The file should be text, separated by ",", ex: nanosecond,nanosecond,1.0")
  println("Please enter the name of the class alignments file (ex: UO-X-SEP-matched-classes.txt):")
  val inputMatchedClasses = scala.io.StdIn.readLine()
  val MatchedClasses: RDD[(String, String)] = sparkSession.sparkContext.textFile("src/main/resources/Matching-result/"+inputMatchedClasses).map(x => (x.split(",").apply(0), x.split(",").apply(1)))
  println("Please enter the name of the relation alignments file (ex: UO-X-SEP-matched-relations.txt):")
  val inputMatchedRelations = scala.io.StdIn.readLine()
  val MatchedRelations: RDD[(String, String)] = sparkSession.sparkContext.textFile("src/main/resources/Matching-result/"+inputMatchedRelations).map(x => (x.split(",").apply(0), x.split(",").apply(1)))
  val R_O1_match = matchStat.getNumberOfO1MatchedClasses(MatchedClasses)+matchStat.getNumberOfO1MatchedRelations(MatchedRelations)
  val R_O2_match = matchStat.getNumberOfO2MatchedClasses(MatchedClasses)+matchStat.getNumberOfO2MatchedRelations(MatchedRelations)
  val numberOfAllMatchedResources = matchStat.getNumberOfAllMatchedResources(MatchedClasses,MatchedRelations)

  /**
    * Get the quality assessment sheet for the input ontologies.
    */
  def GetQualityAssessmentForMatching(O1: RDD[graph.Triple], O2: RDD[graph.Triple]) = {
    println("Degree of overlapping is " + this.DegreeOfOverlapping(O1, O2)*100+"%")
    println("Match coverage is " + this.MatchCoverage(O1, O2))
    println("Match ratio is " + this.MatchRatio(O1, O2))
  }

  /**
    * refers to to how many common resources exist between the input ontologies.
    */
  def DegreeOfOverlapping(O1: RDD[graph.Triple], O2: RDD[graph.Triple]): Double = {
    val Overlapping: Double = numberOfAllMatchedResources / (ontoStat.getAllSchemaResources(O1).count().toDouble + ontoStat.getAllSchemaResources(O2).count().toDouble)
    ontoStat.roundNumber(Overlapping)
//    Overlapping
  }
  /**
    * refers to the fraction of resources which exist in at least one correspondence in the matching results in comparison to the total number of resources in the input ontologies.
    */
  def MatchCoverage(O1: RDD[graph.Triple], O2: RDD[graph.Triple]): Double = {
    val matchCoverage = (R_O1_match + R_O2_match) / (ontoStat.getAllSchemaResources(O1).count().toDouble + ontoStat.getAllSchemaResources(O2).count().toDouble)
    println("R1-match = "+ R_O1_match + " R2-match = "+R_O2_match + " all resources in O1 = "+ ontoStat.getAllSchemaResources(O1).count() + " all resources in O2 = " + ontoStat.getAllSchemaResources(O2).count())
    ontoStat.roundNumber(matchCoverage)
//    matchCoverage
  }
  /**
    * refers to the ratio between the number of found correspondences and the number of matched resources in the input ontologies..
    */
  def MatchRatio(O1: RDD[graph.Triple], O2: RDD[graph.Triple]): Double = {
    val matchRatio: Double = (2 * numberOfAllMatchedResources) / (R_O1_match + R_O2_match)
    ontoStat.roundNumber(matchRatio)
  }
}

