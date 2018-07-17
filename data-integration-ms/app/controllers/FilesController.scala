package controllers

import java.io.File

import javax.inject.Singleton
import play.api.Logger

import scala.util.Try

@Singleton
class FilesController {

  def addFile() = {

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
      val fileName =file.getName()
      val args = fileName.split("-")
      val dsId = args.apply(0)
      if (args.length == 1) false
      else id == dsId.toInt
    }
  }

  def deleteFile(id: String): Boolean = {

    val files = getListFile(id.toInt)
    var deleted = false

    for (file <- files){
      val fileName = file.getName
      Logger.info(s"Found file with id=$id\n -> $fileName")
      if (file.delete()) deleted = true
    }
    deleted
  }

  def mv(oldName: String, newName: String) =
    Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)

  def createDataset(file: File) = {

  }

}
