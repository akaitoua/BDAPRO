package services.spark.example

import org.apache.commons.text.similarity.JaccardDistance
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws}
import services.spark.entity_matchers.EntityMatch
import services.spark.utilities.LookupGenerator

import scala.collection.JavaConversions._


/**
  * This calss is only for development testing purpose,
  * You should application.scala with partitioner type "permutation" to execute the DeletePermutations partitioner.
  */
object DeleteBased extends App {
  val output = "/mnt/work/DI/output"
  val rootPath = "/home/venkat/Downloads/"
  val input1 = rootPath + "company_entities.csv"
  val input2 = rootPath + "test.csv"

  val identityCol = Array("company_name", "country")
  val idCols = concat_ws("", col(identityCol(0)), col(identityCol(1)))

  val conf = new SparkConf()
  conf.set("spark.sql.caseSensitive", "false")
  conf.setMaster("local")
  val spark = SparkSession.builder().appName("Data Integration Microservices").config(conf).getOrCreate()

  val hashtable = LookupGenerator.buildHash(input2);
  val bcHash = spark.sparkContext.broadcast(hashtable)

  val secondDS = spark.sparkContext.textFile(input1)
  val dups = secondDS.map(x => {
    (LookupGenerator.getProbableMatches(x, bcHash.value), x)
    //{val slc = x.split("\\t");x.replace(slc(0),"")})
  }).filter(dup => dup._1.length >= 1)

  val flattedDups: RDD[(String, String)] = dups.flatMap(row => row._1.map((_, row._2)))

  //  flattedDups.take(5).foreach(x=>print(x._1,x._2))

  val dfB: RDD[(String, String)] = spark.sparkContext.textFile(input2).map(x => {
    val slc = x.split("\\t")
    (slc(0), x)
  })

  val joined = dfB.join(flattedDups)

  def produceSimilarity(row1: Array[String], row2: Array[String]): EntityMatch = {
    lazy val jd = new JaccardDistance()
    var sim = 0.0;
    val dataColsLen = row1.length - 1;
    if (row1.length != row2.length) {
      EntityMatch(row1(0) + " len:" + row1.length, row2(0) + " len --> " + row2.length, 100.0)
    } else {
      var simNormal = dataColsLen

      def decSim(): Unit = {
        simNormal -= 1
      }

      for (i <- 1 to dataColsLen) {
        sim += (if (row1(i) != null && row2(i) != null) 1 - jd.apply(row1(i), row2(i)) else {
          decSim();
          0;
        })
      }
      EntityMatch(row1(0), row2(0), sim / simNormal)
    }
  }


  val actualDups = joined.mapValues(x => {
    val ds1 = x._1.split("\\t");
    val ds2 = x._2.split("\\t");
    produceSimilarity(ds1, ds2)
  })

  //print(actualDups.filter( x=>  x._2.similarity.toInt == 1).count())

  actualDups.saveAsTextFile(output)
  spark.stop()
}
