package controllers

import java.nio.file.Paths

import javax.inject._
import play.api.Logger
import play.api.mvc._

@Singleton
class IntegrationController  @Inject()(h2: H2Controller, cc: ControllerComponents) extends AbstractController(cc){

  def index = Action {
    Logger.info("Calling integrations index ...")
    val names = h2.getDatasets()
    Ok(views.html.integrations(names))
  }

  def read(id: String, id_two: String) = Action {Ok(views.html.todo())}

  def update(id: String) = Action {Ok(views.html.todo())}

  def config = Action(parse.multipartFormData)  { request =>
    Logger.info("Calling integrations upload ...")
    val dsOneId = request.body.asFormUrlEncoded("dsOneId").map( { dsOneId => dsOneId.toString.trim}).head
    val dsOneName = h2.getDatasetName("%03d".format(dsOneId.toInt))
    val dsOneFields = h2.getDatasetHeaders(dsOneName).filter(field => field != "DATASET_ID")
    val dsOneDisplayName = dsOneName.replace("_", " ")

    val dsTwoId = request.body.asFormUrlEncoded("dsTwoId").map( { dsTwoId => dsTwoId.toString.trim}).head
    val dsTwoName = h2.getDatasetName("%03d".format(dsTwoId.toInt))
    val dsTwoFields = h2.getDatasetHeaders(dsOneName).filter(field => field != "DATASET_ID")
    val dsTwoDisplayName = dsTwoName.replace("_", " ")

    Ok(views.html.integrate(dsOneDisplayName, dsOneFields, dsTwoDisplayName, dsTwoFields))
  }

  def upload = Action {Ok(views.html.todo())}

  def delete(id: String) = Action {Ok(views.html.todo())}

}
