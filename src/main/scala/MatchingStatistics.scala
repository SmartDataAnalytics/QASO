import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class MatchingStatistics(sparkSession: SparkSession) {


  def getNumberOfAllMatchedClasses (matchedClasses: RDD[(String, String)]): Double ={
    matchedClasses.count().toDouble

  }

  def getNumberOfO1MatchedClasses (matchedClasses: RDD[(String, String)]): Double ={
    matchedClasses.map(x=>x._1).distinct().count().toDouble
  }
  def getNumberOfO2MatchedClasses (matchedClasses: RDD[(String, String)]): Double ={
    matchedClasses.map(x=>x._2).distinct().count().toDouble
  }
  def getNumberOfAllMatchedRelations (matchedRelations: RDD[(String, String)]): Double ={
    matchedRelations.count().toDouble

  }

  def getNumberOfO1MatchedRelations (matchedRelations: RDD[(String, String)]): Double ={
    matchedRelations.map(x=>x._1).distinct().count().toDouble
  }
  def getNumberOfO2MatchedRelations (matchedRelations: RDD[(String, String)]): Double ={
    matchedRelations.map(x=>x._2).distinct().count().toDouble
  }

  def getNumberOfAllMatchedResources (matchedClasses: RDD[(String, String)],matchedRelations: RDD[(String, String)]):Double={
    matchedClasses.count()+matchedRelations.count().toDouble
  }


}
