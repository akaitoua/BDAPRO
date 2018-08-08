import java.awt.image.LookupTable

import SortedNeighborhoodIndex.{dfA, dfB}
import inputreader.{CSVReader, Tuple}
import org.apache.commons.text.similarity.JaccardDistance
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import partitioners.{SortedNeighbor, SoundBased}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import utilities.LookupGenerator

object DeleteBased extends  App{
  val output = "/mnt/work/BDADI/delete"
  val rootPath = "/mnt/work/code-base/IntegrationMicroService/"
  val input1 = rootPath + "company_entities.csv"
  val input2 = rootPath + "company_profiles.csv"
  val identityCol = Array("company_name", "country")
  val idCols = concat_ws("", col(identityCol(0)), col(identityCol(1)))

  val conf = new SparkConf()
  conf.set("spark.sql.caseSensitive", "false")
  conf.setMaster("local")
  val spark = SparkSession.builder().appName("Data Integration Microservices").config(conf).getOrCreate()
  import spark.implicits._

  val csvReader = new CSVReader(spark);
//  val dfA = csvReader.readData(input1, "\t")
//  val dfB = csvReader.readData(input2, "\t")
//  val ds1 = dfA.withColumn("datasetid", lit(1))

  val hashtable = LookupGenerator.buildHash(input2);
  val bcHash = spark.sparkContext.broadcast(hashtable)

  val secondDS = spark.sparkContext.textFile(input1)
  import spark.implicits._
//  val dups = secondDS.map(x=>LookupGenerator.getProbableMatches(x,bcHash.value)).filter(dups => dups.size()>=1)
//  dups.saveAsTextFile(output)
  val dups= secondDS.map(x=>{
    (LookupGenerator.getProbableMatches(x,bcHash.value), x)
      //{val slc = x.split("\\t");x.replace(slc(0),"")})
  }).filter(dup => dup._1.length>=1)

  val flattedDups:RDD[(String,String)] = dups.flatMap(row => row._1.map((_,row._2)))

//  flattedDups.take(5).foreach(x=>print(x._1,x._2))

  val dfB:RDD[(String,String)] = spark.sparkContext.textFile(input2).map(x=>{
    val slc = x.split("\\t")
    (slc(0),x)})

  val joined = dfB.join(flattedDups)
  val soundBased:SoundBased = new SoundBased
  val actualDups = joined.mapValues(x=>{val ds1=x._1.split("\\t"); val ds2=x._2.split("\\t");soundBased.produceSimilarity(ds1,ds2,0.5)})

  actualDups.take(5).foreach(println)

//  actualDups.saveAsTextFile(output)



  // Explode the arraylist to creat join column
//  val dupsExplode = dups.withColumn("joingen",explode(lit(dups.schema.last.name)))


//
//  val isMatch= udf((Id1:String,Id2:String) => {dups.map(x=> if (x.indexOf(0).eq(Id1) && x.contains(Id2)) true else false)
//  val results = dfA.join(dfB,isMatch(dfA("id"),dfB("id")))
//  dups.saveAsTextFile(output)
//  simCalculated.saveAsTextFile(output)
  spark.stop()
}
