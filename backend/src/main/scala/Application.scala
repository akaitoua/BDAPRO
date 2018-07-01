import java.nio.file.{Files, Paths}

import inputreader.CSVReader
import org.apache.commons.text.similarity.JaccardDistance
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import partitioners.SoundBased
import s2cc.Schema2CaseClass

import scala.tools.nsc.transform.patmat.Lit

object Application extends App {

  override def main(args: Array[String]): Unit = {
    val output = "/mnt/work/BDADI/dd"
    val rootPath = "/mnt/work/code-base/IntegrationMicroService/"
    val input1 = rootPath + "company_entities.csv"
    val input2 = rootPath + "company_profiles.csv"
    val identityCol = Array("company_name","country")
    val keyGenCols = concat_ws("",col(identityCol(0)),col(identityCol(1)))


    val conf = new SparkConf()
    conf.set("spark.sql.caseSensitive", "false")
    conf.setMaster("local")

    // Partitioner
    val partitioner = new SoundBased()

    val spark = SparkSession.builder().appName("Data Integration Microservices").config(conf).getOrCreate()
    import spark.implicits._
    val applyPartitioner = udf((x: String) => {
      partitioner.getSoundCode(x)
    })

    val csvReader = new CSVReader(spark);
    val dfA = csvReader.readData(input1, "\t")
    val dfB = csvReader.readData(input2, "\t")
    val ds1 = dfA.withColumn("PartitionCode", applyPartitioner(keyGenCols)).cache().withColumn("datasetid", lit(1))
    val ds2 = dfB.withColumn("PartitionCode", applyPartitioner(keyGenCols)).cache().withColumn("datasetid", lit(2))

    def rowConvert(x:Row,len:Int):Array[String] = {
      var y:Array[String] = new Array[String](len)
      for (i <- 0 to len-1){
        y(i)=String.valueOf(x.get(i))
      }
      y
    }



    val keyedA = ds1.map((row:Row) => (row.getString(row.length-2),rowConvert(row,row.length))).rdd
    val keyedB = ds2.map((row:Row) => (row.getString(row.length-2),rowConvert(row,row.length))).rdd

    val joined = keyedA.join(keyedB)

    case class EntityMatch(e1:String,e2:String,e3:String,e4:String,similarity:Double)

    def produceSimilarity(row1:Array[String],row2:Array[String]):EntityMatch={
      lazy val jd = new JaccardDistance()
      var sim=0.0;
      val dataColsLen= row1.length-1-2; // specific to Soundex Partitioner; -1 to avoid index out of bounds, -2 as we added dataset id, partition code
      var simNormal= dataColsLen
      def decSim(): Unit ={
        simNormal-=1
      }

      for (i <- 1 to dataColsLen ) {
        sim +=  (if(row1(i)!="null" && row2(i) != "null")1 - jd.apply(row1(i), row2(i)) else { decSim();0; })
      }
      EntityMatch(row1(0),row2(0),row1(1),row2(1),sim/simNormal) // remove
    }

    val  simCalculated= joined.mapValues(x=>produceSimilarity(x._2,x._1))

    simCalculated.saveAsTextFile(output)


//    // TODO: Filter single keys/keys in only one dataset and mark no duplicates
//     ls.show(10)

//    case class EntityMatch(e1:String,e2:String,similarity:Double)
////
//    def produceSimilarity(row1:Row):EntityMatch={
//        lazy val jd = new JaccardDistance()
//         val row =EntityMatch(row1.getString(1),row1.getString,1-jd.apply(row1,row2))
//      row
//    }
//
//   val  simCalculated= joinedrdd.mapValues(x=>produceSimilarity(x._2,x._1))
//
//   if (Files.exists(Paths.get(output))){
//     Files.delete(Paths.get(output))
//   }
//   simCalculated.saveAsTextFile(output)

   spark.stop()
  }
}
