package controllers

import java.io.File
import java.nio.file.Paths

import javax.inject._
import models.{Dataset, Integration}
import play.api.Logger
import play.api.mvc._
import services.SoundBased

import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.io.Path
import scala.util.Try

@Singleton
class IntegrationController @Inject()(dsDB: DatasetDBController, intDB: IntegrationDBController,cc: ControllerComponents) extends AbstractController(cc) {

  def index = Action {
    Logger.info("Calling integrations index ...")
    val names = dsDB.show()
    val integrations = intDB.show
    println(integrations)
    Ok(views.html.integrations(names, integrations))
  }

  def show_page(id: Int, pageNumb: Int) = Action {
    val headers = Array("SIMILARITY_ID","ROW_DS_ONE_ID", "ROW_DS_TWO_ID", "SIMILARITY")
    val content = intDB.getIntegrationContent(id, (pageNumb - 1) *10)
    val length = intDB.getLength(id)
    val pages = if (length % 10 == 0) (length / 10) else ((length / 10) + 1)
    Ok(views.html.integration(id, "Demo",headers, content, pageNumb,pages))
  }

  def show(id: Int) = Action {
    Redirect(routes.IntegrationController.show_page(id, 1))
  }

  def show_page_from_form() = Action(parse.multipartFormData) { request =>
    val pageId = request.body.asFormUrlEncoded("pageId").map({ dsOneId => dsOneId.toString.trim }).head
    val pageNumb = request.body.asFormUrlEncoded("pageNumb").map({ dsOneId => dsOneId.toString.trim }).head
    Redirect(routes.IntegrationController.show_page(pageId.toInt, pageNumb.toInt))
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

    val dsOneName = request.body.asFormUrlEncoded("dsOneName").head.replace(" ", "_")
    val dsOneId = request.body.asFormUrlEncoded("dsOneId").head.toInt
    val dsTwoName = request.body.asFormUrlEncoded("dsTwoName").head.replace(" ", "_")
    val dsTwoId = request.body.asFormUrlEncoded("dsTwoId").head.toInt
    val datasetOne = Dataset(dsOneId, dsOneName)
    request.body.asFormUrlEncoded("dsOneField").map({ field => datasetOne.addField(field) })
    val datasetTwo = Dataset(dsTwoId, dsTwoName)
    request.body.asFormUrlEncoded("dsTwoField").map({ field => datasetTwo.addField(field) })

    val integration = Integration(-1, name,datasetOne, datasetTwo, blockingAlg, comparisonAlg, false, threshold)
    intDB.add(integration)

    val currentDirectory = new java.io.File(".").getCanonicalPath
    val dsOne = s"$currentDirectory/datasets/$dsOneName.csv"
    println(dsOne)
    val dsTwo = s"$currentDirectory/datasets/$dsTwoName.csv"
    println(dsTwo)
    val output = s"$currentDirectory/integrations/$name"
    val identityCol = Array("company_name","country")

    val integrationId = intDB.getIdByName(name)

    val soundBased:SoundBased = new SoundBased
    scala.concurrent.Future{
      soundBased.matchEntities(dsOne,dsTwo,output,identityCol,0.5)
      intDB.addSimilarity(integrationId, new File(s"$output/part-00000"))
    }

    Redirect(routes.IntegrationController.index())
  }

  def delete(id: Int) = Action {
    val integration : Integration = intDB.get(id)
    val name = integration.name
    FilesController.deleteIntegration(name)
    intDB.delete(id)
    Redirect(routes.IntegrationController.index())
  }

  def topKL (id: Int, rowId: Int, k: Int) = Action {
    val headers = Array("SIMILARITY_ID","ROW_DS_ONE_ID", "ROW_DS_TWO_ID", "SIMILARITY")
    val idRow = s"ROW_DS_ONE_ID = $rowId "
    val content = intDB.getTopK(id, idRow, k)
    Ok(views.html.topk(id, "Demo",headers, content))
  }

  def topKR (id: Int, rowId: Int, k: Int) = Action {
    val headers = Array("SIMILARITY_ID","ROW_DS_ONE_ID", "ROW_DS_TWO_ID", "SIMILARITY")
    val idRow = s"ROW_DS_TWO_ID = $rowId "
    val content = intDB.getTopK(id, idRow, k)
    Ok(views.html.topk(id, "Demo",headers, content))
  }

}
