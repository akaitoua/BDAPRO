package services.spark.partitioners


import java.io.File

import org.apache.commons.text.similarity.SimilarityScore
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import services.spark.entity_matchers.EntityMatch
import services.spark.utilities.{LookupGenerator, Utilities}

import scala.collection.JavaConversions._

class DeletePermutations extends Serializable {

  def produceSimilarity(row1: Array[String], row2: Array[String], compAlg: String, threshold: Double): Option[EntityMatch] = {
    lazy val dm = Utilities.getDistanceMeasure(compAlg)
    lazy val ed = Utilities.tryEditDistanceMeasure(compAlg)
    if (!dm.isDefined && !ed.isDefined) {
      println("Unidentified edit distance")
      System.exit(0)
    }

    if (row1.length != row2.length || row1.length == 0 || row2.length == 0) {
      None
    } else {

      var sim = 0.0
      val dataColsLen = row1.length - 1
      var simNormal = dataColsLen

      def decSim(): Unit = {
        simNormal -= 1
      }

      for (i <- 1 to dataColsLen) {
        sim += (if (row1(i) != null && row2(i) != null) Utilities.getDistance(row1(i), row2(i), dm, ed) else {
          decSim()
          0
        })
      }
      Some(EntityMatch(row1(0), row2(0), sim / simNormal))
    }
  }


  def matchEntities(input1: String, input2: String, output: String, idcols: Array[String], threshold: Double, compAlg: String, spark: SparkSession):RDD[String] = {
    val file1 = new File(input1)
    val file2 = new File(input2)
    var hashfile = input2
    var iterateFile = input1
    var fileOrderChange = false
    if (file1.length() <= file2.length()) {
      hashfile = input1;
      iterateFile = input2
      fileOrderChange = true
    }

    val hashtable = LookupGenerator.buildHash(hashfile)
    val bcHash = spark.sparkContext.broadcast(hashtable)

    val secondDS = spark.sparkContext.textFile(iterateFile)
    val dups = secondDS.map(x => {
      (LookupGenerator.getProbableMatches(x, bcHash.value), x)
      //{val slc = x.split("\\t");x.replace(slc(0),"")})
    }).filter(dup => dup._1.length >= 1)

    val flattedDups: RDD[(String, String)] = dups.flatMap(row => row._1.map((_, row._2)))

    val dfB: RDD[(String, String)] = spark.sparkContext.textFile(hashfile).map(x => {
      val slc = x.split("\\t")
      (slc(0), x)
    })

    val joined = if (!fileOrderChange) dfB.join(flattedDups) else flattedDups.join(dfB)
    val actualDups = joined.mapValues(x => {
      val ds1 = x._1.split("\\t");
      val ds2 = x._2.split("\\t");
      produceSimilarity(ds1, ds2, compAlg, threshold)
    })
    actualDups.filter(x => x._2.isDefined).map(x => Array(x._2.get.id1, x._2.get.id2, x._2.get.similarity).mkString(","))
  }
}
