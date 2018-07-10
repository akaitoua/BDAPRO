package controllers

import java.io.File

import javax.inject.{Inject, Singleton}
import org.h2.jdbc.JdbcSQLException
import play.api.Logger
import play.api.mvc.{AbstractController, ControllerComponents}
import play.api.db._

import scala.io.Source

@Singleton
class H2Controller {

  def initDB() = {
    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement

    try{
      stmt.execute("CREATE TABLE DATASET (ID INT PRIMARY KEY , NAME VARCHAR(100) UNIQUE )")
    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    }finally {
      conn.close()
    }
  }

  def initDatasets() = {

    val currentDirectory = new java.io.File(".").getCanonicalPath
    val okFileExtensions = List("csv")
    val files = getListOfFiles(new File(s"/$currentDirectory/datasets/"), okFileExtensions)

    for(file <-files){
      uploadToDB(file)
    }

  }

  def uploadToDB(file: File) = {

    var rows : Array[String] = Array[String]()
    val bufferedSource = Source.fromFile(file.getAbsolutePath)
    for(line <- bufferedSource.getLines()){
      rows = rows :+ line
    }
    val args = file.getName.split("-")
    val id = args(0)
    val name = args(1).replace(".csv","")
    val headers = rows.apply(0).split("\t")
    val data = rows.drop(1)

    createTable(name, headers)
    uploadData(id, name, headers, data)
  }

  def uploadData(id: String, name: String, headers: Array[String], data: Array[String]) = {

    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement

    var colNames = "DATASET_ID, "
    for (header <- headers){ colNames += header + ',' }
    colNames = colNames.dropRight(1)

    try {
      stmt.execute(s"INSERT INTO DATASET (id, name) VALUES ('$id', '$name') ;")
      var count = 1
      for (line <- data) {

        var values = s"$count,"

        for (v <- line.split("\t")) {
          if (v != "") values += "\'" + v.replace("'", "''") + "\',"
          else values += "\'none\',"
        }
        values = values.dropRight(1)
        stmt.execute(s"INSERT INTO $name ($colNames) VALUES ($values);")
        count += 1;
      }

    } catch {
        case e: JdbcSQLException => Logger.info(e.getMessage)
    } finally {
      conn.close()
      stmt.close()
    }

  }

  def createTable(name: String, headers: Array[String]) = {

    var cols = "DATASET_ID INT PRIMARY KEY, "
    for(header <- headers){ cols += s"$header VARCHAR(255)," }
    cols = cols.dropRight(1)

    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement

    try{
      stmt.execute(s" DROP TABLE $name")
    }catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    }

    try {
      val stmt = conn.createStatement
      stmt.execute(s"CREATE TABLE $name ($cols);")
    }
    catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    }finally {
      conn.close()
    }

  }

  def dropTable(name: String) = {

    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement

    try{
      stmt.execute(s"DROP TABLE $name;")
      stmt.execute(s"DELETE FROM DATASET WHERE '$name' = NAME;")
    }catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    } finally {
      stmt.close()
      conn.close()
    }

  }

  def getDatasetName(id: String): String = {

    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement

    try{
      val rs = stmt.executeQuery(s"SELECT name FROM DATASET WHERE id=$id")
      if (rs.next()) return rs.getString("name")
      else return null
    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
        return null
    } finally {
      conn.close()
      stmt.close()
    }
  }

  def getDatasetHeaders(name: String): Array[String] ={

    var headers = Array[String]()
    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement

    val dsName = name.toUpperCase
    try {
      val rs = stmt.executeQuery(s"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '$dsName'")
      while (rs.next()){
        headers = headers :+ rs.getString("COLUMN_NAME")
      }
    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    } finally {
      conn.close()
      stmt.close()
    }


    return headers
  }

  def getDatasetLength(name: String): Int ={

    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement
    var lenght = -1

    try{
      val rs = stmt.executeQuery(s"SELECT COUNT(*) as RESULT FROM $name;")
      if (rs.next()) lenght = rs.getInt("RESULT")
    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    } finally {
      conn.close()
      stmt.close()
    }

    return lenght

  }

  def getDatasetContent(name: String, headers: Array[String]): Array[Array[String]] = {

    var rows = Array[Array[String]]()
    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement

    try{
      val rs = stmt.executeQuery(s"SELECT * FROM $name WHERE DATASET_ID < 11")
      while (rs.next()){
        var row = ""
        for (header <- headers){
          row += rs.getString(header) + "\t"
        }
        rows = rows :+ row.split("\t").drop(1)
      }
    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    } finally {
      conn.close()
      stmt.close()
    }

    return  rows
  }

  /*
  Gets the list  of files with a list of extensions at a directory
   */
  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }


}
