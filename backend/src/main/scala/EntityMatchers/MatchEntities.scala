package EntityMatchers

import java.io.File


import partitioners.{SortedNeighbor, SoundBased}
import utilities.Utilities

object MatchEntities {

  def findDuplicates(input1:String,input2:String,output:String,idcol:Array[String],partioner:String,comparisoner:String,threshold:Double)={
    Utilities.deleteRecursively(new File(output))
    if (partioner.equalsIgnoreCase("soundex")){
      val soundPartitioner= new SoundBased
      soundPartitioner.matchEntities(input1,input2,output,idcol,threshold)
    }else if (partioner.equalsIgnoreCase("sortedneighborhood")){
      val sortPartitioner= new SortedNeighbor()
      sortPartitioner.matchEntities(input1,input2,output,idcol,threshold)
    }else {
      printf("Given %s partitioner not support at the moment\n",partioner)
    }
  }
}
