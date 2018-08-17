package services.spark

import java.io.FileWriter

import org.apache.spark.rdd.RDD

object Benchmark {

  def benchmark(simCalculated:RDD[String],input1:String,input2:String,windowSize:Int,comp:String,sim:String,outPath:String): Unit ={
      println("doing benchmark")

      val groupMatches= simCalculated.map(x=> {
        val sim=x.split(",")(2);
        //      println("Similarity "+sim)
        if (sim.startsWith("1.0")) {
          ("1.0",1)
        }else if (sim.startsWith("0.9")) {
          ("0.9",1)
        } else if (sim.startsWith("0.8")) {
          ("0.8",1)
        }else if (sim.startsWith("0.7")) {
          ("0.7",1)
        }else if (sim.startsWith("0.6")) {
          ("0.6",1)
        }else{
          (sim,0)
        }

      }).filter(x=> if (x._1 == "1.0" || x._1 == "0.9" || x._1 == "0.8" || x._1 == "0.7" || x._1== "0.6" ) true else false)
        .reduceByKey(_ + _)

      var sb:StringBuilder=new StringBuilder
      sb.append(input1+","+","+windowSize+","+comp+","+sim)
      sb.append(","+simCalculated.count())
      val fw=new FileWriter(outPath,true)
//      groupMatches.foreach(x=>{print(x._1,x._2);sb.append(","+x._1+","+x._2)})
    groupMatches.collect().foreach(x=>sb.append(","+x._1+","+x._2))
    try{
      fw.write(sb.toString()+"\n")
      println("writing to the benchmark file")
    }finally {
      fw.close()
    }
    }
}
