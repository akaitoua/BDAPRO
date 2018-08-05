package controllers

import java.io.File

import javax.inject.Inject
import models.Dataset
import org.h2.jdbc.JdbcSQLException
import play.api.Logger
import play.api.db._
import play.api.db.evolutions.Evolutions

import scala.io.Source

class DatasetDBController @Inject()(db: Database) {

  // CRUD:

  def add(dataset: Dataset) = {

    createDatasetTable(dataset)
    uploadData(dataset)

  }

  /*
  Get a list of all the Datasets of the system
   */
  def show() = {
    val conn = db.getConnection()
    val stmt = conn.createStatement
    var datasets = Array[Dataset]()

    try {
      val rs = stmt.executeQuery("SELECT * FROM DATASET")
      while (rs.next()) {
        val dsId = rs.getInt("DATASET_ID")
        val dsName = rs.getString("NAME").replace("_", " ")
        datasets = datasets :+ Dataset(dsId, dsName)
      }

    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    } finally {
      stmt.close()
      conn.close()
    }

    datasets
  }

  def read(id: Int, from: Int = 1, to: Int = 10) = {

    val name = getDatasetName(id)
    val ds = Dataset(id, name)
    val fields = getDatasetFields(name)
    fields.map(field => ds.addField(field))
    val data = getDatasetContent(ds, from, to)
    (ds, data)
  }

  def update(id: Int, dataset: Dataset) = {
    delete(id)
    dataset.id = id
    add(dataset)
  }

  def delete(id: Int) = {
    Logger.info(s"DatasetDBController delete action (id:$id) ...")
    val name = getDatasetName(id)
    dropTable(name)
  }

  // DB:


  def dropTable(name: String) = {

    val conn = db.getConnection()
    val stmt = conn.createStatement

    try {
      stmt.execute(s"DROP TABLE $name;")
      stmt.execute(s"DELETE FROM DATASET WHERE '$name' = NAME;")
    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    } finally {
      stmt.close()
      conn.close()
    }

  }

  def renameTable(id: Int, newName: String) = {

    val conn = db.getConnection()
    val stmt = conn.createStatement

    try {
      val oldName = getDatasetName(id)
      stmt.execute(s"ALTER TABLE $oldName RENAME TO $newName;")
      stmt.execute(s"UPDATE DATASET SET NAME = '$newName' WHERE DATASET_ID=$id")
    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    } finally {
      conn.close()
      stmt.close()
    }
  }

  def createDatasetTable(dataset: Dataset) = {

    Logger.info(s"Creating Dataset: $dataset ...")

    var cols = "COLUMN_ID SERIAL PRIMARY KEY, "
    for (field <- dataset.fields) {
      cols += s"$field VARCHAR,"
    }
    cols = cols.dropRight(1)

    val name = dataset.name.replace(" ", "_")
    val size = dataset.data.length
    val conn = db.getConnection()
    val stmt = conn.createStatement

    try {
      stmt.execute(s"DROP TABLE IF EXISTS $name")
      stmt.execute(s"CREATE TABLE $name ($cols);")
      stmt.execute(s"INSERT INTO DATASET (name, size) VALUES ('$name', $size);")

    } catch {
      case e: JdbcSQLException => Logger.error(e.getMessage)
    } finally {
      conn.close()
    }

  }

  def uploadData(dataset: Dataset) = {

    val currentDirectory = new java.io.File(".").getCanonicalPath
    var cols = ""
    for (field <- dataset.fields) {
      cols += s"$field ,"
    }
    cols = cols.dropRight(1)
    val dsName = dataset.name.replace(" ", "_").toLowerCase
    val filePath = s"$currentDirectory/datasets/$dsName.csv"
    val query = s"INSERT INTO $dsName ($cols) SELECT * FROM CSVREAD('$filePath', null, 'fieldSeparator='|| CHAR(9));"

    db.withConnection { conn =>
      val stmt = conn.createStatement()
      stmt.execute(query);
    }


  }


  def getDatasetName(id: Int): String = {

    val conn = db.getConnection()
    val stmt = conn.createStatement

    try {
      val rs = stmt.executeQuery(s"SELECT * FROM DATASET WHERE DATASET_ID=$id")
      if (rs.next()) return rs.getString("NAME")
    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    } finally {
      conn.close()
      stmt.close()
    }

    return ""
  }

  def getDatasetSize(id: Int): Int = {

    val conn = db.getConnection()
    val stmt = conn.createStatement

    try {
      val rs = stmt.executeQuery(s"SELECT * FROM DATASET WHERE DATASET_ID=$id")
      if (rs.next()) return rs.getInt("SIZE")
    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    } finally {
      conn.close()
      stmt.close()
    }

    return -1
  }

  def getDataset(id: Int): Dataset = {

    val conn = db.getConnection()
    val stmt = conn.createStatement

    try {
      val rs = stmt.executeQuery(s"SELECT * FROM DATASET WHERE DATASET_ID=$id")
      while (rs.next()) {
        val name = rs.getString("name")
        val id = rs.getInt("DATASET_ID")
        val ds = Dataset(id, name)
        val fields = getDatasetFields(name)
        fields.map(field => ds.addField(field))
        return ds
      }
      Logger.error("Not found!!")
      return null
    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
        return null
    } finally {
      conn.close()
      stmt.close()
    }
  }

  def getDatasetFields(name: String): Array[String] = {

    var headers = Array[String]()
    val conn = db.getConnection()
    val stmt = conn.createStatement

    val dsName = name.toUpperCase
    try {
      val rs = stmt.executeQuery(s"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '$dsName'")
      while (rs.next()) {
        headers = headers :+ rs.getString("COLUMN_NAME")
      }
    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    } finally {
      conn.close()
      stmt.close()
    }


    return headers.drop(1)
  }

  def getDatasetContent(dataset: Dataset, from: Int = 1, to: Int = 10): Array[Array[String]] = {

    var rows = Array[Array[String]]()
    val conn = db.getConnection()
    val stmt = conn.createStatement
    val name = dataset.name
    try {
      val rs = stmt.executeQuery(s"SELECT * FROM $name WHERE $from <= COLUMN_ID AND COLUMN_ID <= $to")
      while (rs.next()) {
        var row = ""
        for (field <- dataset.fields) {
          row += rs.getString(field) + "\t"
        }
        rows = rows :+ row.split("\t")
      }
    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    } finally {
      conn.close()
      stmt.close()
    }


    return rows
  }


}
