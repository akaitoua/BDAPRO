package models

import java.io.File


case class Dataset(var id: String, var name: String) {

  var fields = Array[String]()
  var data = Array[String]()

  def displayName(): String = {
    name.replace("_", " ")
  }

  def formatId() = {
    this.id = "%03d".format(id.toInt)
  }

  def addField(field: String) = {
    fields = fields :+ field
  }

  def addData(row: String) = {
    data = data :+ row
  }

  def getFileName() = {
    val currentDirectory = new java.io.File(".").getCanonicalPath
    s"/$currentDirectory/datasets/$id-$name.csv"
  }

  override def toString(): String = displayName();
}

