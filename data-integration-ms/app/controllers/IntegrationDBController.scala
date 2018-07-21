package controllers

import org.h2.jdbc.JdbcSQLException
import play.api.Logger
import play.api.db.Databases

class IntegrationDBController {

  def initDB() = {
    Logger.info(s"Initializing DB ...")

    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement

    try {
      stmt.execute("DROP TABLE INTEGRATIONS;")
    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    }

    try {
      stmt.execute("CREATE TABLE INTEGRATION (INTEGRATION_ID VARCHAR(10) PRIMARY KEY , " +
        "DS_ONE VARCHAR(10), DS_TWO VARCHAR(10), BLOCKING_ALG VARCHAR(25), COMPARISON_ALG(25), " +
        "SAME_DS_COMP Boolean, THRESHOLD FLOAT)")
    } catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    } finally {
      conn.close()
    }
  }

}
