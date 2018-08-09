import java.io.File

import inputreader.{CSVReader, Tuple}
import org.apache.commons.text.similarity.JaccardDistance
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.NGram
import org.apache.spark.rdd.RDD
import partitioners.SortedNeighbor
import utilities.Utilities

object SortedNeighborhoodIndex extends App {
  val output = "/mnt/work/BDADI/dd"
  val rootPath = "/mnt/work/code-base/IntegrationMicroService/"
  val input1 = rootPath + "company_entities.csv"
  val input2 = rootPath + "company_profiles.csv"
  val identityCol = Array("company_name", "country")
  val idCols = concat_ws("", col(identityCol(0)), col(identityCol(1)))


  Utilities.deleteRecursively(new File(output))

}