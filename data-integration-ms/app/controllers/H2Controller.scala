package controllers

import javax.inject.{Inject, Singleton}
import play.api.mvc.{AbstractController, ControllerComponents}
import play.api.db._

@Singleton
class H2Controller @Inject()(cc: ControllerComponents) extends AbstractController(cc){

  def index = Action {

    val conn = Databases.inMemory().getConnection()
    try {
      val stmt = conn.createStatement
      /*val rs = stmt.execute("CREATE TABLE demo (" +
        "id int," +
        "name varchar(255)" +
        ");")
      println(rs)*/
      val x = stmt.execute("INSERT INTO demo (id,name) VALUES (1, 'Pablo');")
      println(x)
      val y = stmt.executeQuery("SELECT * FROM demo;")
      while (y.next()) {
        println(y.getString("name"))
      }
    } finally {
      conn.close()
    }

    Ok(views.html.todo())
  }

}
