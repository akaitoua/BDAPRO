package services.spark.inputreader

class Tuple(field:Array[String]) extends Serializable{
  var fields:Array[String]=field

  override def toString ={
    var sb:StringBuilder = new StringBuilder
    for (i<-0 to fields.length-1){
      sb.append(fields(i)+ " ")
    }
    sb.toString()
  }
}

object Tuple{
  def apply(field: Array[String]): Tuple = new Tuple(field)
}
