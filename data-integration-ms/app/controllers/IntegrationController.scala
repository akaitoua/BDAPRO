package controllers

import java.nio.file.Paths

import javax.inject._
import play.Play
import play.api.Logger
import play.api.mvc._

@Singleton
class IntegrationController  @Inject()(cc: ControllerComponents) extends AbstractController(cc){

  def index = Action {Ok(views.html.todo())}

  def read(id: String) = Action {Ok(views.html.todo())}

  def update(id: String) = Action {Ok(views.html.todo())}

  def upload = Action {Ok(views.html.todo())}

  def delete(id: String) = Action {Ok(views.html.todo())}

}
