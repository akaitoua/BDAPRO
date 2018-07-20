import inputreader.{CSVReader, Tuple}
import org.apache.commons.text.similarity.JaccardDistance
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.NGram
import org.apache.spark.rdd.RDD
import partitioners.SortedNeighbor

object SortedNeighborhoodIndex extends App {
  val output = "/mnt/work/BDADI/dd"
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
  val dfA = csvReader.readData(input1, "\t")
  val dfB = csvReader.readData(input2, "\t")
  val ds1 = dfA.withColumn("datasetid", lit(1))
  val ds2 = dfB.withColumn("datasetid", lit(2))

  def rowConvert(x:Row,len:Int):Tuple = {
    var y:Array[String] = new Array[String](len)
    for (i <- 0 to len-1){
      y(i)=String.valueOf(x.get(i))
    }
    Tuple(y)
  }

  val sn = new SortedNeighbor(4)

  val combined=ds1.union(ds2).sort(idCols).rdd.map(row=> rowConvert(row,row.length))
  val sortedNBlocked = sn.getBlockedRDD(combined)


//  sortedNBlocked.take(15).foreach(x=>println(x._1.toString+" second --> "+x._2.toString))
//
//  spark.stop()
//
//  System.exit(0)

  case class EntityMatch(id1:String,id2:String,name1:String,name2:String,similarity:Double)

  def produceSimilarity(row1:Array[String],row2:Array[String]):Option[EntityMatch]={
    lazy val jd = new JaccardDistance()
    var sim=0.0;
    val dataColsLen= row1.length-1-1; // specific to Soundex Partitioner; -1 to avoid index out of bounds, -2 as we added dataset id, partition code

    if (row1(dataColsLen+1)!=row2(dataColsLen+1)) {
      var simNormal = dataColsLen

      def decSim(): Unit = {
        simNormal -= 1
      }

      for (i <- 1 to dataColsLen) {
        sim += (if (row1(i) != "null" && row2(i) != "null") 1 - jd.apply(row1(i), row2(i)) else {
          decSim();
          0;
        })
      }
      Some(EntityMatch(row1(0), row2(0), row1(1), row2(1), sim / simNormal)) // remove
    }else{
      None
    }
  }

  val  simCalculated= sortedNBlocked.map(x=>produceSimilarity(x._2.fields,x._1.fields)).filter(x=>{ x != None}).map(x=>(Array(x.get.id1,x.get.id2,x.get.similarity.toString).mkString(",")))

  simCalculated.saveAsTextFile(output)
  spark.stop()

  //  val combinedrdd=combined.map(row=> rowConvert(row,row.length)).zipWithIndex().map{case (k,v) => (v,k)}
//  combined.take(2)
//  spark.stop()
}