package controllers

import java.io.File

import javax.inject.Singleton

@Singleton
class FilesController {

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

  def createDataset(file: File) = {

  }

}
