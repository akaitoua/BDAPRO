import java.nio.file.{Files, Paths}

import inputreader.CSVReader
import org.apache.commons.text.similarity.JaccardDistance
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import partitioners.SoundBased

import scala.tools.nsc.transform.patmat.Lit

object Application extends App {

  override def main(args: Array[String]): Unit = {
    val output = "/mnt/work/DI/output"
    val rootPath = "/mnt/work/code-base/IntegrationMicroService/"
    val input1 = rootPath + "company_entities.csv"
    val input2 = rootPath + "company_profiles.csv"
    val identityCol = Array("company_name","country")

    val soundBased:SoundBased = new SoundBased
    soundBased.matchEntities(input1,input2,output,identityCol,0.5)
  }
}
