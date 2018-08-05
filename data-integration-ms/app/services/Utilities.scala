package services

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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

}
