package services.spark.utilities

import java.io.File

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import services.spark.inputreader.CSVReader

class Utilities extends Serializable {

  def rowConvert(x:Row,len:Int):Array[String] = {
    var y:Array[String] = new Array[String](len)
    for (i <- 0 to len-1){
      y(i)=String.valueOf(x.get(i))
    }
    y
  }

  def readInputAsDataFrame(file1:String,file2:String,spark:SparkSession):Array[DataFrame]={
    val csvReader = new CSVReader(spark);
    val dfA = csvReader.readData(file1, "\t")
    val dfB = csvReader.readData(file2, "\t")
    Array(dfA,dfB)
  }

}

object Utilities {
  def deleteRecursively(file: File):Unit={
    //    val file:File= new File(fileName);
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

}