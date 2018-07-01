import inputreader.CSVReader
import org.apache.commons.text.similarity.JaccardDistance
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SparkSession}
import org.apache.spark.sql.functions._
import partitioners.SoundBased

import scala.tools.nsc.transform.patmat.Lit

object Application extends App {

  override def main(args: Array[String]): Unit = {
    val rootPath = "/mnt/work/code-base/IntegrationMicroService/"
    val input1 = rootPath + "company_entities.csv"
    val input2 = rootPath + "company_profiles.csv"
    val identityCol = "company_name"

    val conf = new SparkConf()
    conf.setMaster("local")


    case class Company(id:Int,name:String,alt_name:String,url:String,founded:Int,city:String,country:String,PartitionCode:String,datasetid:Int)


    // Partitioner
    val partitioner = new SoundBased()
    val applyPartitioner = udf((x: String) => partitioner.getSoundCode(x))

    val spark = SparkSession.builder().appName("Data Integration Microservices").config(conf).getOrCreate()
    import spark.implicits._

    val csvReader = new CSVReader(spark);
    val datasetA = csvReader.readData(input1, "\t")
    val datasetB = csvReader.readData(input2, "\t")
    val datasetAPart = datasetA.withColumn("PartitionCode", applyPartitioner(datasetA(identityCol))).cache().withColumn("datasetid", lit(1))
    val datasetBPart = datasetA.withColumn("PartitionCode", applyPartitioner(datasetA(identityCol))).cache().withColumn("datasetid", lit(2))

    val keyedA = datasetAPart.map(row => (row.getString(row.length-2),row.getString(1))).rdd
    val keyedB = datasetBPart.map(row => (row.getString(row.length-2),row.getString(1))).rdd
    // TODO: Filter single keys/keys in only one dataset and mark no duplicates

//    def produceSimilarity(row1:Row,row2:Row):Row={
//
//      if( row1.getString(row1.length-2) == row2.getString(row2.length-2)){ Row.empty } else {
//        lazy val jd = new JaccardDistance()
//        val row= RowFactory.create(row1.get(0).toString,row2.get(0).toString,
//          jd.apply(row1.get(1).toString,row2.get(1).toString))
//        row
//      }
//    }



    val joined = keyedA.join(keyedB)

    case class EntityMatch(e1:String,e2:String,similarity:Double)

    def produceSimilarity(row1:String,row2:String):EntityMatch={
        lazy val jd = new JaccardDistance()
         val row =EntityMatch(row1,row2,1-jd.apply(row1,row2))
      row
    }

   val  simCalculated= joined.mapValues(x=>produceSimilarity(x._2,x._1))
//
//    val reducerudf = udf(produceSimilarity(_,_))

//    val mergedDataSet:DataFrame= datasetAPart.union(datasetBPart)
//    val byPartitionCodeKey = mergedDataSet.map(row => (row.getString(row.length-2),row.getString(1))).rdd.reduceByKey((v,v1)=>(if (v.length<v1.length) v else v1))
//    val keySim = byPartitionCodeKey.collect()
    simCalculated.saveAsTextFile("/mnt/work/BDADI/Metaphone-Jaccard.txt")
    //reduce((x,y)=>produceSimilarity(x,y) )


//    val count = mergedDataSet.groupBy("PartitionCode").count()
//    println(count.count())
//    val sorted = mergedDataSet.repartition($"PartitionCode")
//
//    println(sorted.rdd.getNumPartitions)

//    val mergedData = mergedDataSet.as[Company]

//    val keyed= mergedData.map(comp => (comp.PartitionCode,comp))
//    val keyedData = mergedDataSet.map(row => (row.getString(row.length-2),row))
//    keyedData.show(5)


   spark.stop()
  }
}
