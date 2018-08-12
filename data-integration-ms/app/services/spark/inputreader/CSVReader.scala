package services.spark.inputreader


import org.apache.spark.sql.{DataFrame, SparkSession}

class CSVReader(spark: SparkSession) {
  def readData(input: String, delimiter: String, header: Boolean): DataFrame = spark.read.format("csv")
    .option("header", header.toString).option("inferSchema", "true").option("delimiter", delimiter).load(input)

  def readData(input: String): DataFrame = readData(input, ",", true)

  def readData(input: String, delimiter: String): DataFrame = readData(input, delimiter, true)
}
