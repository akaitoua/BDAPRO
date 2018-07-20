package partitioners


import inputreader.Tuple
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._

class SortedNeighbor (windowSize:Int = 4) extends Serializable {

  def buildBlockingKey(row:Tuple):String={
    row.fields(1) // TODO: do dynamic blocking key
  }


  def getBlockedRDD(rows:RDD[Tuple]):RDD[(Tuple,Tuple)]= {
    val blockingKey:RDD[(String,Tuple)] = rows.map(row => {
      (buildBlockingKey(row),row)
    })
    val sortBlockKey = blockingKey.sortByKey().map(_._2)
    cartesianWindow(sortBlockKey.sliding(windowSize).map(_.toSeq))
  }

  /**
    * creates cartesian pairs of rows
    * */
  def cartesianWindow(rdd:RDD[Seq[Tuple]]):RDD[(Tuple,Tuple)]={
    rdd.flatMap(block=> {
      val maxIndex = block.length-1
      for (first <- 0 to maxIndex; second<-first+1 to maxIndex) yield (block(first),block(second))
    })
  }
}
