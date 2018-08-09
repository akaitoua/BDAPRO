package controllers

import java.io.File

import javax.inject.Inject
import models.{Dataset, Integration}
import org.h2.jdbc.JdbcSQLException
import play.api.Logger
import play.api.db.Database

import scala.io.Source

class IntegrationDBController @Inject()(db: Database, dsDBController: DatasetDBController){

  def add(integration: Integration) = {
    println(integration)
    val values = integration.getValues
    val query = s"INSERT INTO INTEGRATION (INTEGRATION_NAME, DS_ONE_ID, DS_TWO_ID, BLOCKING_ALG, COMPARISON_ALG, SAME_DS_COMP, THRESHOLD) VALUES ($values);"

    db.withConnection { conn =>
      val stmt = conn.createStatement()
      stmt.execute(query);
    }
  }

  def show : Array[Integration] =  {
    var integrations = Array[Integration]()
    val query = s"SELECT * FROM INTEGRATION;"

    db.withConnection { conn =>
      val rs = conn.createStatement().executeQuery(query);
      while (rs.next()) {

        val id = rs.getInt("INTEGRATION_ID")
        val name = rs.getString("INTEGRATION_NAME")
        val dsOneId = rs.getInt("DS_ONE_ID")
        val dsTwoId = rs.getInt("DS_ONE_ID")
        val block = rs.getString("BLOCKING_ALG")
        val comp = rs.getString("COMPARISON_ALG")
        val same = rs.getBoolean("SAME_DS_COMP")
        val threshold = rs.getFloat("THRESHOLD")

        val dsOne = dsDBController.getDataset(dsOneId)
        val dsTwo = dsDBController.getDataset(dsTwoId)

        integrations = integrations :+ Integration(id, name, dsOne, dsTwo, block, comp, same, threshold)

      }

    }
    integrations
  }

  def get(id: Int) : Integration = {

    val query = s"SELECT * FROM INTEGRATION WHERE INTEGRATION_ID = $id; "

    db.withConnection { conn =>
      val rs = conn.createStatement().executeQuery(query);
      while (rs.next()) {

        val id = rs.getInt("INTEGRATION_ID")
        val name = rs.getString("INTEGRATION_NAME")
        val dsOneId = rs.getInt("DS_ONE_ID")
        val dsTwoId = rs.getInt("DS_ONE_ID")
        val block = rs.getString("BLOCKING_ALG")
        val comp = rs.getString("COMPARISON_ALG")
        val same = rs.getBoolean("SAME_DS_COMP")
        val threshold = rs.getFloat("THRESHOLD")

        val dsOne = dsDBController.getDataset(dsOneId)
        val dsTwo = dsDBController.getDataset(dsTwoId)

        return Integration(id, name, dsOne, dsTwo, block, comp, same, threshold)
      }
    }
    null

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

  def getIdByName(name: String): Int = {

    val query = s"SELECT INTEGRATION_ID FROM INTEGRATION WHERE INTEGRATION_NAME = '$name'; "

    db.withConnection { conn =>
      val stmt = conn.createStatement()
      val res = stmt.executeQuery(query);
      if(res.next()) res.getInt("INTEGRATION_ID")
      else -1
    }

  }

  def getLength(id: Int) = {

    val query = s"SELECT COUNT(*) FROM SIMILARITY WHERE INTEGRATION_ID = $id; "

    db.withConnection { conn =>
      val stmt = conn.createStatement()
      val res = stmt.executeQuery(query);
      if(res.next()) res.getInt("COUNT(*)")
      else -1
    }
  }

  def getTopK(id: Int, idRow: String, topK: Int ) : Array[Array[String]] = {

    var rows = Array[Array[String]]()
    val query = s"SELECT SIMILARITY_ID, ROW_DS_ONE_ID, ROW_DS_TWO_ID, SIMILARITY FROM SIMILARITY " +
      s"WHERE INTEGRATION_ID = $id AND " +
      s"$idRow " +
      s"ORDER BY SIMILARITY DESC LIMIT $topK;"

    db.withConnection { conn =>
      val stmt = conn.createStatement()
      val res = stmt.executeQuery(query);
      while(res.next()) {
        var row = ""
        for (field <- Array("SIMILARITY_ID", "ROW_DS_ONE_ID", "ROW_DS_TWO_ID", "SIMILARITY")) {
          row += res.getString(field) + "\t"
        }
        rows = rows :+ row.split("\t")
      }

    }

    return rows
  }

  def getThreshold(id: Int) : Float = {

    val query = s"SELECT THRESHOLD FROM INTEGRATION WHERE INTEGRATION_ID = $id; "

    db.withConnection { conn =>
      val stmt = conn.createStatement()
      val res = stmt.executeQuery(query);
      if(res.next()) res.getFloat("THRESHOLD")
      else -1
    }

  }


}
