package controllers

import java.nio.file.Paths

import javax.inject._
import models.{Dataset, Integration}
import play.api.Logger
import play.api.mvc._

@Singleton
class IntegrationController @Inject()(dsDB: DatasetDBController, cc: ControllerComponents) extends AbstractController(cc) {

  def index = Action {
    Logger.info("Calling integrations index ...")
    val names = dsDB.show()
    Ok(views.html.integrations(names))
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

    Ok(views.html.integrate(dsOne, dsTwo))
  }

  def upload = Action(parse.multipartFormData) { request =>

    val blockingAlg = request.body.asFormUrlEncoded("blockingAlg").head
    val comparisonAlg = request.body.asFormUrlEncoded("comparisonAlg").head
    val sameDSComp = request.body.asFormUrlEncoded("sameDSComp").head
    val threshold = request.body.asFormUrlEncoded("threshold").head.toFloat

    val dsOneName = request.body.asFormUrlEncoded("dsOneName").head.replace(" ", "_").toUpperCase
    val dsTwoName = request.body.asFormUrlEncoded("dsTwoName").head.replace(" ", "_").toUpperCase
    val datasetOne = Dataset(-1, dsOneName)
    request.body.asFormUrlEncoded("dsOneField").map({ field => datasetOne.addField(field) })
    val datasetTwo = Dataset(-1, dsTwoName)
    request.body.asFormUrlEncoded("dsTwoField").map({ field => datasetTwo.addField(field) })

    val integration = Integration(datasetOne, datasetTwo, blockingAlg, comparisonAlg, false, threshold)
    Redirect(routes.IntegrationController.index())
  }

  def delete(id: Int) = Action {
    Ok(views.html.todo())
  }

}
