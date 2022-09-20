import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph
import org.apache.jena.riot.Lang
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object QualityAssessment {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkSession1 = SparkSession.builder
      .master("local[*]").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()

    val startTimeMillis = System.currentTimeMillis()

//    println("Please add the two ontologies to the ontology directory")
//    println("Please enter the name of the first ontology file (ex: UO.ttl):")
//    val inputOntology1 = scala.io.StdIn.readLine()
//    val O1 = "src/main/resources/Ontology/"+inputOntology1
    val O1 = "src/main/resources/Ontology/PO.ttl"

//    println("Please enter the name of the second ontology file (ex: SEP.ttl):")
//    val inputOntology2 = scala.io.StdIn.readLine()
//    val O2 = "src/main/resources/Ontology/"+inputOntology2
      val O2 = "src/main/resources/Ontology/SEP.ttl"

    val lang1: Lang = Lang.TURTLE

    val O1triples: RDD[graph.Triple] = sparkSession1.rdf(lang1)(O1).distinct(2)
    val O2triples: RDD[graph.Triple] = sparkSession1.rdf(lang1)(O2).distinct(2)

    val ontStat = new OntologyStatistics(sparkSession1)
    println("Statistics for O1 ontology")
    ontStat.getStatistics(O1triples)
    println("Statistics for O2 ontology")
    ontStat.getStatistics(O2triples)

    println("==========================================================================")
    println("|         Quality Assessment for input ontologies        |")
    println("==========================================================================")
    val Oquality = new QualityAssessmentForInputOntology(sparkSession1)
    val Mquality = new QualityAssessmentForMatchingProcess(sparkSession1)
    println("Quality Assessment for O1:")
    Oquality.GetQualityAssessmentForOntology(O1triples)
    println("Quality Assessment for O2:")
    Oquality.GetQualityAssessmentForOntology(O2triples)
    println("==========================================================================")
    println("|         Quality Assessment for the matching process        |")
    println("==========================================================================")
    Mquality.GetQualityAssessmentForMatching(O1triples,O2triples)


    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / (1000)
    val durationMinutes = (endTimeMillis - startTimeMillis) / (1000 * 60)
    println("runtime = " + durationSeconds + " seconds")
    println("runtime = " + durationMinutes + " minutes")
    sparkSession1.stop


  }
}