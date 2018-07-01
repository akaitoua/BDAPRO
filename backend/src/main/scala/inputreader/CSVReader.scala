package inputreader

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
class CSVReader(spark:SparkSession) {
   def readData(input:String,delimiter:String): DataFrame =spark.read.format("csv")
     .option("header","true").option("inferSchema","true").option("delimiter",delimiter).load(input)

   def readData(input:String):DataFrame = readData(input,",")
}
