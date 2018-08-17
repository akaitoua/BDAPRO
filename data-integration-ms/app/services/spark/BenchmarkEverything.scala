package services.spark

import services.spark.entity_matchers.EntityMatch

object BenchmarkEverything extends App {


  // partioners - soundex, sortedneighborhood, permutations
  // distances Jacccard, Jaro-winkler, Levenshtein
  override def main(args: Array[String]): Unit = {

    val fileLocation="/mnt/work/code-base/dima-pro/data-integration-ms/data/"
    val datafiles:Array[Array[String]] = Array(Array("original10.tsv", "modified10.tsv"),Array("original20.tsv", "modified20.tsv"))
    val windowSizes = Array(4, 8, 12); //applicable only for sorted neighborhood

    val comparisonAlg = Array("sortedneighborhood","permutations")
    val simMeasures = Array("Jaccard", "Jaro-winkler", "Levenshtein")

    //  val comparisonAlg = Array("sortedneighborhood", "permutations")
    //  val simMeasures = Array("Jaccard", "Jaro-winkler", "Levenshtein")
    val idCols = Array("PRIMARYTITLE","ORIGINALTITLE")
    val threshold = 0.0
    // Do window different windows for sortedneighborhood
    for (bmark <- datafiles) {
      for (i <- comparisonAlg) { // comparison permutations
        for (j <- simMeasures) {
          var window = 0
          if (i == "sortedneighborhood") {
            for (k <- windowSizes) {
              window = k
              doBenchmark(fileLocation + bmark(0), fileLocation + bmark(1), window, threshold, idCols, i, j)
            }
          } else {
            doBenchmark(fileLocation + bmark(0), fileLocation + bmark(1), window, threshold, idCols, i, j)
          }
        }
      }
    }
  }


  def doBenchmark(input: String, input2: String, windowSize: Int, th: Double, idCol: Array[String], comp: String, sim: String): Unit = {
    EntityMatch.doWork(input, input2, "", idCol, comp, sim, th, true, windowSize)
  }

}
