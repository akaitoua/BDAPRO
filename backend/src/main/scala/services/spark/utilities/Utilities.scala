package services.spark.utilities

import java.io.File

import services.spark.inputreader.CSVReader
import org.apache.commons.text.similarity._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class Utilities extends Serializable {

  def rowConvert(x: Row, len: Int): Array[String] = {
    var y: Array[String] = new Array[String](len)
    for (i <- 0 to len - 1) {
      y(i) = String.valueOf(x.get(i))
    }
    y
  }

  def readInputAsDataFrame(file1: String, file2: String, spark: SparkSession): Array[DataFrame] = {
    val csvReader = new CSVReader(spark);
    val dfA = csvReader.readData(file1, "\t")
    val dfB = csvReader.readData(file2, "\t")
    Array(dfA, dfB)
  }

}

object Utilities {
  def deleteRecursively(file: File): Unit = {
    //    val file:File= new File(fileName);
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  def getDistanceMeasure(alg: String): Option[SimilarityScore[java.lang.Double]] = {
    if (alg.equalsIgnoreCase("jaro-winkler")) {

      //      println("Returning Jaro distance measure")
      Some(new JaroWinklerDistance())
    }
    //    else if (alg.equalsIgnoreCase("levenshtein")) {
    //      new LevenshteinDistance
    //    }
    else if (alg.equalsIgnoreCase("jaccard")) {
      //      println("Returning Jaccard distance measure")
      Some(new JaccardDistance());
    } else {
      None
    }
  }

  def tryEditDistanceMeasure(alg: String): Option[SimilarityScore[java.lang.Integer]] = {
    if (alg.equalsIgnoreCase("levenshtein")) {
      //      println("Return Levenshtein distance")
      Some(new LevenshteinDistance)
    } else {
      None
    }
  }


  def getDistance(a: String, b: String, dm: Option[SimilarityScore[java.lang.Double]], ed: Option[SimilarityScore[java.lang.Integer]]): Double = {
    if (ed.isDefined) {
      val maxLen =  math.max(a.length,b.length);
      1-(math.abs(ed.get.apply(a, b)).toDouble / (if (maxLen==0) 1 else maxLen))
    } else if (dm.isDefined) {
      if (dm.get.isInstanceOf[JaroWinklerDistance]){
        dm.get.apply(a, b) // library is returning similarity instead of distance
      }else {
        1 - dm.get.apply(a, b)
      }
    } else {
      println("Similarity measure missing")
      0
    }
  }
}
