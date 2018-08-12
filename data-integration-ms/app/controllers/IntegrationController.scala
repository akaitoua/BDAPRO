package controllers

import java.io.File

import javax.inject._
import models.{Dataset, Integration}
import org.joda.time.{DateTime, DateTimeZone}
import play.api.Logger
import play.api.mvc._
import services.spark.entity_matchers.EntityMatch

import scala.concurrent.ExecutionContext.Implicits.global

@Singleton
class IntegrationController @Inject()(dsDB: DatasetDBController, intDB: IntegrationDBController,cc: ControllerComponents) extends AbstractController(cc) {

  def index = Action {
    Logger.info("Calling integrations index ...")
    val names = dsDB.show()
    val integrations = intDB.getIntegrations
    println(integrations)
    Ok(views.html.integrations(names, integrations))
  }

  def show_page(id: Int, pageNumb: Int) = Action {
    val headers = Array("SIMILARITY_ID","ROW_DS_ONE_ID", "ROW_DS_TWO_ID", "SIMILARITY")
    val threshold = intDB.getIntegrationThreshold(id);
    val content = intDB.getIntegrationContent(id, threshold,(pageNumb - 1) *10)
    val length = intDB.getInterationLength(id)
    val pages = if (length % 10 == 0) (length / 10) else ((length / 10) + 1)
    val integration = intDB.getIntegration(id)
    Ok(views.html.integration(integration,headers, content, pageNumb,pages))
  }

  def show(id: Int) = Action {
    Redirect(routes.IntegrationController.show_page(id, 1))
  }

  def show_page_from_form() = Action(parse.multipartFormData) { request =>
    val pageId = request.body.asFormUrlEncoded("pageId").map({ dsOneId => dsOneId.toString.trim }).head
    val pageNumb = request.body.asFormUrlEncoded("pageNumb").map({ dsOneId => dsOneId.toString.trim }).head
    Redirect(routes.IntegrationController.show_page(pageId.toInt, pageNumb.toInt))
  }

  def upload = Action(parse.multipartFormData) { request =>

    val name = request.body.asFormUrlEncoded("integration_name").head

    val blockingAlg = request.body.asFormUrlEncoded("blockingAlg").head
    val comparisonAlg = request.body.asFormUrlEncoded("comparisonAlg").head
    val threshold = request.body.asFormUrlEncoded("threshold").head.toFloat

    val dsOneName = request.body.asFormUrlEncoded("dsOneName").head.replace(" ", "_").toLowerCase
    val dsOneId = request.body.asFormUrlEncoded("dsOneId").head.toInt
    val dsTwoName = request.body.asFormUrlEncoded("dsTwoName").head.replace(" ", "_").toLowerCase
    val dsTwoId = request.body.asFormUrlEncoded("dsTwoId").head.toInt
    val datasetOne = Dataset(dsOneId, dsOneName)
    request.body.asFormUrlEncoded("dsOneField").map({ field => datasetOne.addField(field) })
    val datasetTwo = Dataset(dsTwoId, dsTwoName)
    request.body.asFormUrlEncoded("dsTwoField").map({ field => datasetTwo.addField(field) })

    val integration = Integration(-1, name,datasetOne, datasetTwo, blockingAlg, comparisonAlg, threshold)
    intDB.addIntegration(integration)

    val currentDirectory = new java.io.File(".").getCanonicalPath

    val output = s"$currentDirectory/integrations/$name"
    val identityCol = datasetOne.fields

    val integrationId = intDB.getIntegrationId(name)


    scala.concurrent.Future{
      print("Generating data!...")
      dsDB.generateDatasetFile(datasetOne.name.toLowerCase, datasetOne.fields)
      dsDB.generateDatasetFile(datasetTwo.name.toLowerCase, datasetTwo.fields)
      print("Generating data... Done!")

      val dsOne = s"$currentDirectory/data/$dsOneName.tsv"
      println(dsOne)
      val dsTwo = s"$currentDirectory/data/$dsTwoName.tsv"
      println(dsTwo)

      println("Matching! ...")
      val start = DateTime.now(DateTimeZone.UTC).getMillis()
      EntityMatch.findDuplicates(dsOne,dsTwo,output,identityCol,blockingAlg,comparisonAlg,threshold)
      val end = DateTime.now(DateTimeZone.UTC).getMillis()
      println(s"Matching... Done! in ${end-start} ms")

      print("Adding results to the DB ...")
      FilesController.getListOfSparkFiles(name).foreach( file => {
        intDB.addSimilarity(integrationId, file)
      })
      print("Adding results to the DB ... Done!")
      intDB.setIntegrationReady(integrationId, true)
    }

    Redirect(routes.IntegrationController.index())
  }

  def delete(id: Int) = Action {
    val integration : Integration = intDB.getIntegration(id)
    val name = integration.name
    FilesController.deleteIntegration(name)
    intDB.delete(id)
    Redirect(routes.IntegrationController.index())
  }

  def topKL (id: Int, rowId: Int, k: Int) = Action {
    val integration = intDB.getIntegration(id)
    val headers = "COLUMN_ID" +: integration.datasetOne.fields :+ "SIMILARITY"
    val mainRow = dsDB.getDatasetRow(integration.datasetTwo.id, rowId)
    val content = intDB.getIntegrationTopK(id, rowId, fromDSOne = true,topK = k)
    Ok(views.html.topk(id, integration.name, headers, mainRow,content))
  }

  def topKR (id: Int, rowId: Int, k: Int) = Action {
    val integration = intDB.getIntegration(id)
    val headers = "COLUMN_ID" +: integration.datasetOne.fields :+ "SIMILARITY"
    val mainRow = dsDB.getDatasetRow(integration.datasetOne.id, rowId)
    val content = intDB.getIntegrationTopK(id, rowId, fromDSOne = false,topK = k)
    Ok(views.html.topk(id, integration.name, headers, mainRow, content))
  }

  def benchmark(id: Int) = Action {
    println("Calling benchmark!")
    scala.concurrent.Future{
      val thresholds = for (i <- 6 to 10) yield i.toFloat / 10
      thresholds.foreach(t => {
        val benchmark = intDB.benchmark(id, t)
        println(s"$t -> $benchmark")
      })

      val comparisons = intDB.getInterationLength(id)
      println(s"Comparisons -> $comparisons")
    }
    Redirect(routes.IntegrationController.show(id))
  }

}
