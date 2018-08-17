package services.spark.entity_matchers

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import services.spark.Benchmark
import services.spark.partitioners.{DeletePermutations, SortedNeighbor, SoundBased}
import services.spark.utilities.Utilities

import scala.io.Source

case class EntityMatch(id1:String,id2:String,similarity:Double)

object EntityMatch {

  def findDuplicates(input1:String,input2:String,output:String,idcol:Array[String],partioner:String,comparisoner:String,threshold:Double,windowSize:Int)={
    doWork(input1,input2,output,idcol,partioner,comparisoner,threshold,false,windowSize)
  }
  def doWork(input1:String,input2:String,output:String,idcol:Array[String],partioner:String,comparisoner:String,threshold:Double,benchmark:Boolean,windowSize:Int)={
    Utilities.deleteRecursively(new File(output))
    val conf = new SparkConf()
    conf.set("spark.sql.caseSensitive", "false")
    conf.setMaster("local")
    printf("Spark: executing partitioner '%s' and comparisoner '%s' \n",partioner,comparisoner)
    val header = Source.fromFile(input1)
    val spark = SparkSession.builder().appName("Data Integration Microservices").config(conf).getOrCreate()
    var simCalculated:RDD[String]=spark.sparkContext.emptyRDD[String]
    if (partioner.equalsIgnoreCase("soundex")){
      val soundPartitioner= new SoundBased
      simCalculated=soundPartitioner.matchEntities(input1,input2,output,idcol,comparisoner,threshold,spark)
    }else if (partioner.equalsIgnoreCase("sortedneighborhood")){
      val sortPartitioner= new SortedNeighbor()
      simCalculated=sortPartitioner.matchEntities(input1,input2,output,idcol,comparisoner,threshold,spark,windowSize)
    }else if(partioner.equalsIgnoreCase("permutations")){
      val deletePerms = new DeletePermutations
      simCalculated=deletePerms.matchEntities(input1,input2,output,idcol,threshold,comparisoner,spark)
    } else {
      printf("Given %s partitioner not support at the moment\n",partioner)
      spark.close();
      println("Spark: finished execution aborted")
      System.exit(1)
    }
    if(benchmark){
      Benchmark.benchmark(simCalculated,input1,input2,windowSize,partioner,comparisoner)
    }else{
      simCalculated.saveAsTextFile(output)
    }
    spark.close();
    println("Spark: finished execution")
  }
}