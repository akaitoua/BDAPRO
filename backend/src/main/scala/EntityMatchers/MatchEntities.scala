package EntityMatchers

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import partitioners.{DeletePermutations, SortedNeighbor, SoundBased}
import utilities.Utilities

object MatchEntities {

  def findDuplicates(input1:String,input2:String,output:String,idcol:Array[String],partioner:String,comparisoner:String,threshold:Double)={
    Utilities.deleteRecursively(new File(output))
    val conf = new SparkConf()
    conf.set("spark.sql.caseSensitive", "false")
    conf.setMaster("local")
    val spark = SparkSession.builder().appName("Data Integration Microservices").config(conf).getOrCreate()
    if (partioner.equalsIgnoreCase("soundex")){
      val soundPartitioner= new SoundBased
      soundPartitioner.matchEntities(input1,input2,output,idcol,threshold)
    }else if (partioner.equalsIgnoreCase("sortedneighborhood")){
      val sortPartitioner= new SortedNeighbor()
      sortPartitioner.matchEntities(input1,input2,output,idcol,threshold)
    }else if(partioner.equalsIgnoreCase("permutations")){
      val deletePerms = new DeletePermutations
      deletePerms.matchEntities(input1,input2,output,idcol,threshold,spark)
    } else {
      printf("Given %s partitioner not support at the moment\n",partioner)
    }
    spark.close();
  }
}
