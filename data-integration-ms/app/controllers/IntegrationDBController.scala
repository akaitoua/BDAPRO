package controllers

import java.io.File

import javax.inject.Inject
import models.{Integration}
import org.h2.jdbc.JdbcSQLException
import play.api.{Logger}
import play.api.db.Database

import scala.io.Source

class IntegrationDBController @Inject()(db: Database, dsDBController: DatasetDBController){

  /*
  Adds a new integration to the DB.
   */
  def addIntegration(integration: Integration) = {

    val values = integration.getValues
    val query = s"INSERT INTO INTEGRATION (INTEGRATION_NAME, DS_ONE_ID, DS_TWO_ID, BLOCKING_ALG, COMPARISON_ALG, THRESHOLD) VALUES ($values);"

    db.withConnection { conn =>
      val stmt = conn.createStatement()
      stmt.execute(query);
    }
  }

  /*
  Gets an specific integration with a given id
   */
  def getIntegration(id: Int) : Integration = {

    val query = s"SELECT * FROM INTEGRATION WHERE INTEGRATION_ID = $id; "

    db.withConnection { conn =>
      val rs = conn.createStatement().executeQuery(query);
      while (rs.next()) {

        val id = rs.getInt("INTEGRATION_ID")
        val name = rs.getString("INTEGRATION_NAME")
        val dsOneId = rs.getInt("DS_ONE_ID")
        val dsTwoId = rs.getInt("DS_TWO_ID")
        val block = rs.getString("BLOCKING_ALG")
        val comp = rs.getString("COMPARISON_ALG")
        val threshold = rs.getFloat("THRESHOLD")
        val ready = rs.getBoolean("READY")

        val dsOne = dsDBController.getDataset(dsOneId)
        val dsTwo = dsDBController.getDataset(dsTwoId)

        return Integration(id, name, dsOne, dsTwo, block, comp, threshold, ready)
      }
    }
    null

  }

  /*
  Returns an array with all the integrations stored in the db
   */
  def getIntegrations : Array[Integration] =  {
    var integrations = Array[Integration]()
    val query = s"SELECT * FROM INTEGRATION;"

    db.withConnection { conn =>
      val rs = conn.createStatement().executeQuery(query);
      while (rs.next()) {

        val id = rs.getInt("INTEGRATION_ID")
        val name = rs.getString("INTEGRATION_NAME")
        val dsOneId = rs.getInt("DS_ONE_ID")
        val dsTwoId = rs.getInt("DS_TWO_ID")
        val block = rs.getString("BLOCKING_ALG")
        val comp = rs.getString("COMPARISON_ALG")
        val threshold = rs.getFloat("THRESHOLD")
        val ready = rs.getBoolean("READY")

        val dsOne = dsDBController.getDataset(dsOneId)
        val dsTwo = dsDBController.getDataset(dsTwoId)

        integrations = integrations :+ Integration(id, name, dsOne, dsTwo, block, comp, threshold, ready)

      }

    }
    integrations
  }

  def getIntegrationId(name: String): Int = {

    val query = s"SELECT INTEGRATION_ID FROM INTEGRATION WHERE INTEGRATION_NAME = '$name'; "

    db.withConnection { conn =>
      val stmt = conn.createStatement()
      val res = stmt.executeQuery(query);
      if(res.next()) res.getInt("INTEGRATION_ID")
      else -1
    }

  }

  def getInterationLength(id: Int) = {
    val query = s"SELECT COUNT(*) FROM SIMILARITY " +
      s"WHERE INTEGRATION_ID = $id AND SIMILARITY >= ${getIntegrationThreshold(id)}; "

    db.withConnection { conn =>
      val stmt = conn.createStatement()
      val res = stmt.executeQuery(query);
      if(res.next()) res.getInt("COUNT(*)")
      else -1
    }
  }

  def getIntegrationThreshold(id: Int) : Float = {

    val query = s"SELECT THRESHOLD FROM INTEGRATION WHERE INTEGRATION_ID = $id; "

    db.withConnection { conn =>
      val stmt = conn.createStatement()
      val res = stmt.executeQuery(query);
      if(res.next()) res.getFloat("THRESHOLD")
      else -1
    }

  }

  /*
  Get the content of a given integration with a given threshold.
   */
  def getIntegrationContent(id: Int, threshold: Float,offset: Int = 0, limit: Int = 10): Array[Array[String]] = {

    var rows = Array[Array[String]]()
    val conn = db.getConnection()
    val stmt = conn.createStatement
    try {
      val rs = stmt.executeQuery(s"SELECT * FROM SIMILARITY WHERE INTEGRATION_ID = $id  AND SIMILARITY >= $threshold OFFSET $offset LIMIT $limit")
      while (rs.next()) {
        var row = ""
        for (field <- Array("SIMILARITY_ID", "ROW_DS_ONE_ID", "ROW_DS_TWO_ID", "SIMILARITY")) {
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

  /*
  Gets an Integration comparison of a given row.
   */
  def getIntegrationTopK(id: Int, rowId: Int, fromDSOne: Boolean = true, topK: Int = 5) : Array[Array[String]] = {

    val integration = getIntegration(id)
    val fields = integration.datasetOne.fields
    val dsOneName = integration.datasetOne.name
    val dsTwoName = integration.datasetTwo.name

    var rows = Array[Array[String]]()
    val fieldsStr = fields.mkString(",")
    val query = s"SELECT COLUMN_ID, $fieldsStr, SIMILARITY FROM SIMILARITY " +
      s"JOIN ${if (fromDSOne) dsOneName else dsTwoName} ON COLUMN_ID = ${if (fromDSOne) "ROW_DS_TWO_ID" else "ROW_DS_ONE_ID"} " +
      s"WHERE INTEGRATION_ID = $id AND ${if (fromDSOne) "ROW_DS_ONE_ID" else "ROW_DS_TWO_ID"} = $rowId " +
      s"ORDER BY SIMILARITY DESC LIMIT $topK;"

    db.withConnection { conn =>
      val stmt = conn.createStatement()
      val res = stmt.executeQuery(query);
      while(res.next()) {
        var row = ""
        for (field <- "COLUMN_ID" +: fields :+ "SIMILARITY") {
          row += res.getString(field) + "\t"
        }
        rows = rows :+ row.split("\t")
      }
    }

    return rows
  }

  def isIntegrationReady(id: Int) : Boolean = {
    val query = s"SELECT READY FROM INTEGRATION WHERE INTEGRATION_ID = $id; "

    db.withConnection { conn =>
      val stmt = conn.createStatement()
      val res = stmt.executeQuery(query);
      if(res.next()) res.getBoolean("READY")
      else false
    }
  }

  def setIntegrationReady(id: Int, ready: Boolean) = {
    val query = s"UPDATE INTEGRATION SET READY = '$ready' WHERE INTEGRATION_ID = $id;"
    db.withConnection { conn =>
      val stmt = conn.createStatement()
      stmt.execute(query);
    }
  }


  def delete(id: Int) =  {

    val query = s"DELETE FROM INTEGRATION WHERE INTEGRATION_ID = $id; "

    db.withConnection { conn =>
      val stmt = conn.createStatement()
      stmt.execute(query);
    }

  }

  def addSimilarity(integrationId: Int, file : File) = {

    val bufferedSource = Source.fromFile(file.getAbsolutePath)
    for (line <- bufferedSource.getLines()) {
      val data = line.split(",")
      db.withConnection { conn =>
        val stmt = conn.createStatement()
        val rowDsOneId = data(0)
        val rowDsTwoId = data(1)
        val sim = data(2)
        val query = s"INSERT INTO SIMILARITY (INTEGRATION_ID, ROW_DS_ONE_ID, ROW_DS_TWO_ID, SIMILARITY) " +
          s"VALUES ($integrationId, $rowDsOneId, $rowDsTwoId, $sim);"
        stmt.execute(query);
      }
    }

  }

}
