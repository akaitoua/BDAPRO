package controllers

import javax.inject.Inject
import models.Integration
import play.api.db.{Database}

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

}
