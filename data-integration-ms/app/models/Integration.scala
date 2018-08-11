package models

case class Integration(id:Int, name: String, datasetOne: Dataset, datasetTwo: Dataset, blocking: String, comparison: String, threshold: Float, ready: Boolean = false) {

  def getValues : String = {
    val dsOne = datasetOne.id
    val dsTwo = datasetTwo.id
    s"'$name', $dsOne, $dsTwo, '$blocking', '$comparison', $threshold"
  }

}
