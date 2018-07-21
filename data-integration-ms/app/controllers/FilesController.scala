package controllers

import java.io.{File, FileWriter, PrintWriter}

import javax.inject.Singleton
import models.Dataset
import play.api.Logger

import scala.io.Source
import scala.util.Try

@Singleton
class FilesController {

  def createDataset(file: File): Dataset = {

    var rows: Array[String] = Array[String]()
    val bufferedSource = Source.fromFile(file.getAbsolutePath)
    for (line <- bufferedSource.getLines()) {
      rows = rows :+ line
    }

    val name = file.getName.replace(".csv", "")
    val fields = rows.apply(0).split("\t")
    val data = rows.drop(1)

    val ds = Dataset(-1, name)
    fields.map(field => ds.addField(field))
    data.map(row => ds.addData(row))
    ds
  }

  /*
  Gets the list  of files with a list of extensions at a directory
   */
  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

  def getFiles() = {
    val currentDirectory = new java.io.File(".").getCanonicalPath
    val okFileExtensions = List("csv")
    getListOfFiles(new File(s"/$currentDirectory/datasets/"), okFileExtensions)
  }

  /*
  Get a files with an specific id
   */
  def getListFile(id: Int): List[File] = {
    val currentDirectory = new java.io.File(".").getCanonicalPath
    val dir = new File(s"/$currentDirectory/datasets/")

    dir.listFiles.filter(_.isFile).toList.filter { file =>
      val fileName = file.getName()
      val args = fileName.split("-")
      val dsId = args.apply(0)
      if (args.length == 1) false
      else id == dsId.toInt
    }
  }

  def deleteFile(name: String): Boolean = {

    val currentDirectory = new java.io.File(".").getCanonicalPath
    val file = new File(s"/$currentDirectory/datasets/$name.csv")
    file.delete()

  }

  def mv(oldName: String, newName: String) =
    Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)

  def appendLine(filePath: String, lines: Array[String]): Unit = {
    val pw = new PrintWriter(new File(filePath))
    try {
      lines.foreach(line => pw.write(s"$line\n"))
    } finally {
      pw.close
    }
  }

}
