package services.spark.example

import services.spark.entity_matchers.EntityMatch


object Application extends App {

  override def main(args: Array[String]): Unit = {
    val output = "/mnt/work/DI/output"
//        val rootPath = "/mnt/work/code-base/IntegrationMicroService/"
    val rootPath = "/home/venkat/Downloads/"
    val input1 = rootPath + "company_entities.csv"
    val input2 = rootPath + "test.csv"
    val identityCol = Array("company_name", "country")

    // currently only supports
    // partioners - soundex, sortedneighborhood, permutations
    // distances Jacccard, Jaro-winkler, Levenshtein
    // all name ares NOT case sensitive
    EntityMatch.findDuplicates(input1, input2, output, identityCol, "permutations", "levenshtein", 0.5)
  }
}
