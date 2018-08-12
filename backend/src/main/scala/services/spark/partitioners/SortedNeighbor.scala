package services.spark.partitioners


import services.spark.inputreader.{CSVReader, Tuple}
import org.apache.commons.text.similarity.JaccardDistance
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import services.spark.utilities.Utilities

class SortedNeighbor(windowSize: Int = 4) extends Serializable {

  def buildBlockingKey(row: Tuple): String = {
    row.fields(2) // TODO: do dynamic blocking key
  }


  def getBlockedRDD(rows: RDD[Tuple]): RDD[(Tuple, Tuple)] = {
    val blockingKey: RDD[(String, Tuple)] = rows.map(row => {
      (buildBlockingKey(row), row)
    })
    val sortBlockKey = blockingKey.sortByKey().map(_._2)
    cartesianWindow(sortBlockKey.sliding(windowSize,windowSize).map(_.toSeq)).filter(x=>x._1.fields(x._1.len()-1)!="2" && x._1.fields(x._1.len()-1)!=x._2.fields(x._2.len()-1))//x=>(x(0).fields(x(0).len()-1)!="2" && x(0).fields(x(0).len()-1)!=x(1).fields(x(1).len()-1)))
  }

  /**
    * creates cartesian pairs of rows
    **/
  def cartesianWindow(rdd: RDD[Seq[Tuple]]): RDD[(Tuple, Tuple)] = {
    rdd.flatMap(block => {
      val maxIndex = block.length - 1
      for (first <- 0 to maxIndex; second <- first + 1 to maxIndex) yield (block(first), block(second))
    })
  }


  def matchEntities(input1: String, input2: String, output: String, idcols: Array[String], compAlg: String, threshold: Double) = {
    val idc=idcols:+"datasetid"
    val idCols = concat_ws("", idc.map(x => col(x)): _*)
    val conf = new SparkConf()
    conf.set("spark.sql.caseSensitive", "false")
    conf.setMaster("local")
    val spark = SparkSession.builder().appName("Data Integration Microservices").config(conf).getOrCreate()
    import spark.implicits._

    val csvReader = new CSVReader(spark);
    val dfA = csvReader.readData(input1, "\t")
    val dfB = csvReader.readData(input2, "\t")
    val ds1 = dfA.withColumn("datasetid", lit(1))
    val ds2 = dfB.withColumn("datasetid", lit(2))

    def rowConvert(x: Row, len: Int): Tuple = {
      var y: Array[String] = new Array[String](len)
      for (i <- 0 until len) {
        y(i) = String.valueOf(x.get(i))
      }
      Tuple(y)
    }

    val sn = new SortedNeighbor(4)

    val combined = ds1.union(ds2).sort(idCols).rdd.map(row => rowConvert(row, row.length))
    val sortedNBlocked = sn.getBlockedRDD(combined)

//    sortedNBlocked.take(100).foreach(x=>println(x._1.toString+" second --> "+x._2.toString))
    //  sortedNBlocked.take(15).foreach(x=>println(x._1.toString+" second --> "+x._2.toString))
    //
    //  spark.stop()
    //
    //  System.exit(0)

    case class EntityMatch(id1: String, id2: String, name1: String, name2: String, similarity: Double)

    def produceSimilarity(row1: Array[String], row2: Array[String]): Option[EntityMatch] = {
      lazy val jd = Utilities.getDistanceMeasure(compAlg)
      var sim = 0.0;
      val dataColsLen = row1.length - 1 - 1; // specific to Soundex Partitioner; -1 to avoid index out of bounds, -2 as we added dataset id, partition code

      if (row1(dataColsLen + 1) != row2(dataColsLen + 1)) {
        var simNormal = dataColsLen

        def decSim(): Unit = {
          simNormal -= 1
        }

        for (i <- 1 to dataColsLen) {
          sim += (if (row1(i) != "null" && row2(i) != "null") 1 - jd.apply(row1(i), row2(i)) else {
            decSim();
            0;
          })
        }
        Some(EntityMatch(row1(0), row2(0), row1(1), row2(1), sim / simNormal)) // remove
      } else {
        None
      }
    }

    val simCalculated = sortedNBlocked.map(x => produceSimilarity(x._2.fields, x._1.fields)).filter(x => {
      x.isDefined
    }).map(x => Array(x.get.id1, x.get.id2, x.get.similarity.toString).mkString(","))

//    simCalculated.take(20).foreach(println)
    simCalculated.saveAsTextFile(output)
    spark.stop()
  }
}
