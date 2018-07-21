package models

case class Integration(datasetOne: Dataset, datasetTwo: Dataset, blocking: String, comparison: String, sameDSComparison: Boolean, threshold: Float) {

}
