package services.spark.partitioners

import services.spark.entity_matchers.EntityMatch
import org.apache.commons.codec.language._
import org.apache.commons.text.similarity.SimilarityScore
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, concat_ws, lit, udf}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import services.spark.utilities.Utilities

class SoundBased extends Serializable {

 @transient lazy val metaphone = new Metaphone()

  def getSoundCode(col: String): String = if (col != "null") metaphone.encode(col) else col

  def createPartition(dfA: DataFrame, dfB: DataFrame, keyGenCols: Column): Array[DataFrame] = {
    val ds1 = dfA.withColumn("PartitionCode", generateJoinSoundCode(keyGenCols)).cache()
    val ds2 = dfB.withColumn("PartitionCode", generateJoinSoundCode(keyGenCols)).cache()
    Array(ds1, ds2)
  }

  def produceSimilarity(row1: Array[String], row2: Array[String], compAlg: String, threshold: Double): EntityMatch = {
    //    lazy val dm = Utilities.getDistanceMeasure(compAlg).get
    val dm = Utilities.getDistanceMeasure(compAlg)
    val ed = Utilities.tryEditDistanceMeasure(compAlg)
    if (!dm.isDefined && !ed.isDefined) {
      println("Unidentified edit distance")
      System.exit(0)
    }

    var sim = 0.0;
    val dataColsLen = row1.length - 1 - 2; // specific to Soundex Partitioner; -1 to avoid index out of bounds, -2 as we added dataset id, partition code
    var simNormal = dataColsLen

    def decSim(): Unit = {
      simNormal -= 1
    }

    for (i <- 1 to dataColsLen) {
      sim += (if (row1(i) != "null" && row2(i) != "null") Utilities.getDistance(row1(i), row2(i), dm, ed) else {
        decSim();
        0;
      })
    }
    EntityMatch(row1(0), row2(0), sim / simNormal)
  }

  val generateJoinSoundCode = udf((x: String) => {
    getSoundCode(x)
  })


  def matchEntities(input1: String, input2: String, output: String, idcols: Array[String], compAlg: String, threshold: Double, spark: SparkSession): RDD[String] = {
    val keyGenCols = concat_ws("", idcols.map(x => col(x)): _*)
//    val keyGenCols = col(idcols(2))
    // TODO: Get idcols for blocking from UI
    import spark.implicits._
    var utilities: Utilities = new Utilities();
    // Partitioner
    val partitioner = new SoundBased()


    val inputs = utilities.readInputAsDataFrame(input1, input2, spark)

    val soundKeyAddedDF = partitioner.createPartition(inputs(0).withColumn("datasetid", lit(1)), inputs(1).withColumn("datasetid", lit(2)), keyGenCols)
    val keyedA = soundKeyAddedDF(0).map((row: Row) => (row.getString(row.length - 1), utilities.rowConvert(row, row.length))).rdd
    val keyedB = soundKeyAddedDF(1).map((row: Row) => (row.getString(row.length - 1), utilities.rowConvert(row, row.length))).rdd

    val joined = keyedA.join(keyedB)

    val simCalculated = joined.mapValues(x => partitioner.produceSimilarity(x._2, x._1, compAlg, threshold)).map(x => (Array(x._2.id1, x._2.id2, x._2.similarity.toString).mkString(",")));
    simCalculated
    //    simCalculated.saveAsTextFile(output)
//    .benchmark(simCalculated)
  }
}
