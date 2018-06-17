package controllers

import java.nio.file.Paths

import javax.inject._
import play.Play
import play.api.Logger
import play.api.mvc._

@Singleton
class DatasetController  @Inject()(cc: ControllerComponents) extends AbstractController(cc){

  def index = Action {Ok(views.html.todo())}

  def read(id: String) = Action {Ok(views.html.todo())}

  def update(id: String) = Action {Ok(views.html.todo())}

  def upload = Action(parse.multipartFormData) { request =>
    request.body.file("dataset").map { dataset =>

      // only get the last part of the filename
      // otherwise someone can send a path like ../../home/foo/bar.txt to write to other files on the system
      val filename = Paths.get(dataset.filename).getFileName
      var appPath = Play.application().path()
      Logger.info(s"$appPath")

      dataset.ref.moveTo(Paths.get(s"/$appPath/datasets/$filename"), replace = true)
      Logger.info(s"File added!")
      Ok("File uploaded")
    }.getOrElse {
      Logger.info(s"File not added!")
      Redirect(routes.HomeController.index).flashing(
        "error" -> "Missing file")
    }
  }

  def delete(id: String) = Action {Ok(views.html.todo())}

}
