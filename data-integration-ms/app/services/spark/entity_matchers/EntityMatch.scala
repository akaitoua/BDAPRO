package services.spark.entity_matchers

import java.io.File

import services.spark.partitioners.{SortedNeighbor, SoundBased}
import services.spark.utilities.Utilities

case class EntityMatch(id1:String,id2:String,similarity:Double)
object EntityMatch {

  def findDuplicates(input1:String,input2:String,output:String,idcol:Array[String],partioner:String,comparisoner:String,threshold:Double)={
    Utilities.deleteRecursively(new File(output))

    if (partioner.equalsIgnoreCase("soundex")){
      println("Calling sound base!")
      val soundPartitioner= new SoundBased
      soundPartitioner.matchEntities(input1,input2,output,idcol,threshold)
      println("Finished!")
    }else if (partioner.equalsIgnoreCase("sortedneighborhood")){
      println("Calling sound Sorted Neighbor!")
      val sortPartitioner= new SortedNeighbor()
      sortPartitioner.matchEntities(input1,input2,output,idcol,threshold)
      println("Finished!")
    }else {
      printf("Given %s partitioner not support at the moment\n",partioner)
    }
  }
}
