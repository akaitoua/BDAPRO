package controllers

import java.io.File

import javax.inject.Inject
import models.Dataset
import org.h2.jdbc.JdbcSQLException
import play.api.Logger
import play.api.db.Databases

import scala.io.Source

class DatasetDBController {

  // CRUD:

  def add(dataset: Dataset) = {
    createDatasetTable(dataset)
    uploadData(dataset)
  }

  /*
  Get a list of all the Datasets of the system
   */
  def show() = {
    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement
    var datasets = Array[Dataset]()

    try {
      val rs = stmt.executeQuery("SELECT * FROM DATASET")
      while (rs.next()){
        val dsId = rs.getString("ID")
        val dsName = rs.getString("NAME").replace("_", " ")
        datasets = datasets :+ Dataset(dsId,dsName)
      }

    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    } finally {
      stmt.close()
      conn.close()
    }

    datasets
  }

  def read(id : String, from: Int = 1, to: Int = 10) = {

    val name = getDatasetName(id)
    val ds = Dataset(id, name)
    val fields = getDatasetFields(name)
    fields.map(field => ds.addField(field))
    val data = getDatasetContent(ds, from, to)
    (ds, data)
  }

  def update(id: String, dataset: Dataset) = {
    delete(id)
    dataset.id = id
    add(dataset)
  }

  def delete(id: String) = {
    val name = getDatasetName(id)
    dropTable(name)
  }

  // DB:

  def initDB() = {
    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement

    try{
      stmt.execute("CREATE TABLE DATASET (ID VARCHAR(10) PRIMARY KEY , NAME VARCHAR(100) UNIQUE )")
    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    }finally {
      conn.close()
    }
  }

  def initDatasets(files : List[File]) = {

    for(file <-files){
      print(file)
      val ds = createFromFile(file)
      createDatasetTable(ds)
      uploadData(ds)
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

  def renameTable(id: String, newName: String) = {

    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement
    val dsId = "%03d".format(id.toInt)
    try{
      val oldName = getDatasetName(dsId)
      stmt.execute(s"ALTER TABLE $oldName RENAME TO $newName;")
      stmt.execute(s"UPDATE DATASET SET NAME = '$newName' WHERE ID = $dsId")
    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    } finally {
      conn.close()
      stmt.close()
    }
  }

  def createDatasetTable(dataset: Dataset) = {

    var cols = "COLUMN_ID INT PRIMARY KEY, "
    for(field <- dataset.fields){ cols += s"$field VARCHAR(255)," }
    cols = cols.dropRight(1)
    val name = dataset.name

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

  def uploadData(dataset: Dataset) = {

    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement
    val id = dataset.id
    val name = dataset.name

    var colNames = "COLUMN_ID, "
    for (field <- dataset.fields){ colNames += field + ',' }
    colNames = colNames.dropRight(1)

    try {
      stmt.execute(s"INSERT INTO DATASET (id, name) VALUES ('$id', '$name') ;")
      var count = 1
      for (line <- dataset.data) {

        var values = s"$count,"

        for (v <- line.split("\t")) {
          if (v != "") values += "\'" + v.replace("'", "''") + "\',"
          else values += "\' \',"
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

  def createFromFile(file: File) : Dataset = {

    var rows : Array[String] = Array[String]()
    val bufferedSource = Source.fromFile(file.getAbsolutePath)
    for(line <- bufferedSource.getLines()){
      rows = rows :+ line
    }
    val args = file.getName.split("-")
    val id = args(0)
    val name = args(1).replace(".csv","")
    val fields = rows.apply(0).split("\t")
    val data = rows.drop(1)


    val ds = Dataset(id, name)
    fields.map(field => ds.addField(field))
    data.map(row => ds.addData(row))
    ds
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

  def getDatasetFields(name: String): Array[String] ={

    var headers = Array[String]()
    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement

    val dsName = name.toUpperCase
    try {
      val rs = stmt.executeQuery(s"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '$dsName'" )
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

  def getDatasetContent(dataset: Dataset, from: Int = 1, to: Int = 10): Array[Array[String]] = {

    var rows = Array[Array[String]]()
    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement
    val name = dataset.name
    try{
      val rs = stmt.executeQuery(s"SELECT * FROM $name WHERE $from <= COLUMN_ID AND COLUMN_ID <= $to")
      while (rs.next()){
        var row = ""
        for (field <- dataset.fields){
          row += rs.getString(field) + "\t"
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


}
