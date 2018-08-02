package controllers

import java.nio.file.Paths

import javax.inject._
import models.{Dataset, Integration}
import play.api.Logger
import play.api.mvc._

@Singleton
class IntegrationController @Inject()(dsDB: DatasetDBController, intDB: IntegrationDBController,cc: ControllerComponents) extends AbstractController(cc) {

  def index = Action {
    Logger.info("Calling integrations index ...")
    val names = dsDB.show()
    val integrations = intDB.show
    println(integrations)
    Ok(views.html.integrations(names, integrations))
  }

  def read(id: Int, id_two: Int) = Action {
    Ok(views.html.todo())
  }

  def update(id: Int) = Action {
    Ok(views.html.todo())
  }

  def config = Action(parse.multipartFormData) { request =>
    Logger.info("Calling integrations upload ...")
    val dsOneId = request.body.asFormUrlEncoded("dsOneId").map({ dsOneId => dsOneId.toString.trim }).head
    val dsOneName = dsDB.getDatasetName(dsOneId.toInt)
    val dsOneFields = dsDB.getDatasetFields(dsOneName).filter(field => field != "DATASET_ID")
    val dsOne: Dataset = Dataset(dsOneId.toInt, dsOneName)
    dsOneFields.map(field => dsOne.addField(field))

    val dsTwoId = request.body.asFormUrlEncoded("dsTwoId").map({ dsTwoId => dsTwoId.toString.trim }).head
    val dsTwoName = dsDB.getDatasetName(dsTwoId.toInt)
    val dsTwoFields = dsDB.getDatasetFields(dsTwoName).filter(field => field != "DATASET_ID")
    val dsTwo: Dataset = Dataset(dsTwoId.toInt, dsTwoName)
    dsTwoFields.map(field => dsTwo.addField(field))

    Ok(views.html.integrate("Some name",dsOne, dsTwo))
  }

  def upload = Action(parse.multipartFormData) { request =>

    val name = request.body.asFormUrlEncoded("integration_name").head

    val blockingAlg = request.body.asFormUrlEncoded("blockingAlg").head
    val comparisonAlg = request.body.asFormUrlEncoded("comparisonAlg").head
    val sameDSComp = request.body.asFormUrlEncoded("sameDSComp").head
    val threshold = request.body.asFormUrlEncoded("threshold").head.toFloat

    val dsOneName = request.body.asFormUrlEncoded("dsOneName").head.replace(" ", "_").toUpperCase
    val dsOneId = request.body.asFormUrlEncoded("dsOneId").head.toInt
    val dsTwoName = request.body.asFormUrlEncoded("dsTwoName").head.replace(" ", "_").toUpperCase
    val dsTwoId = request.body.asFormUrlEncoded("dsTwoId").head.toInt
    val datasetOne = Dataset(dsOneId, dsOneName)
    request.body.asFormUrlEncoded("dsOneField").map({ field => datasetOne.addField(field) })
    val datasetTwo = Dataset(dsTwoId, dsTwoName)
    request.body.asFormUrlEncoded("dsTwoField").map({ field => datasetTwo.addField(field) })

    val integration = Integration(-1, name,datasetOne, datasetTwo, blockingAlg, comparisonAlg, false, threshold)
    intDB.add(integration)
    Redirect(routes.IntegrationController.index())
  }

  def delete(id: Int) = Action {
    Ok(views.html.todo())
  }

}
