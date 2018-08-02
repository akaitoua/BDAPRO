package controllers

import java.io.File
import java.nio.file.Paths

import scala.io.Source
import javax.inject._
import play.api.Logger
import play.api.libs.json.Json
import play.api.mvc._
import play.api.libs.json._

import scala.concurrent.ExecutionContext.Implicits.global
import models.Dataset
import org.h2.jdbc.JdbcSQLException
import play.api.db.Databases

import scala.util.Try


@Singleton
class DatasetController @Inject()(cc: ControllerComponents, db: DatasetDBController, fc: FilesController) extends AbstractController(cc) {


  /*
  Returns a json files with the information of all existing dataset files
  Responsible for the GET /api/datasets route
   */
  def index = Action {
    Logger.info("Calling index action ...")

    val files = fc.getFiles()
    val datasets = db.show()
    //datasets.map(dataset => dataset.formatId())
    Ok(views.html.index(datasets))
  }

  /*
  Responsible for the download of a given file
   */
  def download(id: Int) = Action {
    Logger.info(s"Calling download action (id:$id) ...")
    val dataset = db.getDataset(id)

    if (dataset == null) NotFound(views.html.todo())
    else Ok.sendFile(new java.io.File(dataset.getFileName()))
  }

  def update(id: Int) = Action(parse.multipartFormData) { request =>

    Logger.info("Calling update action ...")

    val currentDirectory = new java.io.File(".").getCanonicalPath
    var name = ""

    request.body.asFormUrlEncoded("datasetName").map({ datasetName =>
      name = datasetName.toString.trim
    })

    val filePath = s"$currentDirectory/datasets/$name.csv"

    request.body.file("dataset").map { dataset =>


      val fileName = dataset.filename
      val oldName = db.getDatasetName(id)
      val oldFile = s"$currentDirectory/datasets/$oldName.csv"

      if (fileName == "") {
        Logger.info(s"File not found im form")
        fc.mv(oldFile, filePath)
        db.renameTable(id, name)

      } else {
        Logger.info(s"Uploading file: $fileName")
        fc.deleteFile(oldName)
        dataset.ref.moveTo(Paths.get(filePath), replace = true)
        Logger.info(s"File $name.csv added!")

        val newFile = fc.getListFile(id.toInt).head
        db.update(id, fc.createDataset(newFile))
      }

    }

    Redirect(routes.DatasetController.index)
  }

  /*
  Uploads a new dataset file
  Responsible for the POST /api/dataset route
   */
  def upload = Action(parse.multipartFormData) { request =>

    Logger.info("Calling upload action ...")

    val currentDirectory = new java.io.File(".").getCanonicalPath
    val okFileExtensions = List("csv")
    val files = fc.getListOfFiles(new File(s"/$currentDirectory/datasets/"), okFileExtensions)

    val name = request.body.asFormUrlEncoded("datasetName").map({ datasetName =>
      datasetName.toString.replace(" ", "_").trim
    }).head

    Logger.info("New dataset name: " + name)

    request.body.file("dataset").map { dataset =>

      val fileName = dataset.filename
      Logger.info(s"Uploading file: $fileName")

      dataset.ref.moveTo(Paths.get(s"/$currentDirectory/datasets/$name.csv"), replace = true)
      Logger.info(s"File $name.csv added!")

      Logger.info("Uploading dataset to H2 ...")
      val ds = fc.createDataset(new File(s"/$currentDirectory/datasets/$name.csv"))
      db.add(ds)
      Logger.info("Dataset uploaded to H2")
      Redirect(routes.DatasetController.index)
    }.getOrElse {
      Logger.error(s"File not found im form")
      Redirect(routes.DatasetController.index)
        .flashing(
          "error" -> "Missing file")
    }
  }

  /*
  Deletes a dataset file with a given id
  Responsible for the DELETE /api/dataset/:id route
   */
  def delete(id: Int) = Action {
    Logger.info(s"Calling delete action (id:$id) ...")
    val dsName = db.getDatasetName(id)

    if (fc.deleteFile(dsName)) {
      db.delete(id)
      Redirect(routes.DatasetController.index)
    }
    else NotFound(views.html.todo())
  }


  def show_page(id: Int, pagNumb: Int) = Action {
    val from = 1 + 10 * (pagNumb - 1)
    val to = 10 * pagNumb
    val t = db.read(id, from, to)
    val ds: Dataset = t._1
    val rows = t._2
    val length = db.getDatasetSize(id)
    println(s"!!!!$length")

    val pages = if (length % 10 == 0) (length / 10) else ((length / 10) + 1)
    Ok(views.html.dataset(id, ds.displayName(), ds.fields, rows, pagNumb, pages))
  }

  def show(id: Int) = Action {
    Redirect(routes.DatasetController.show_page(id, 1))
  }

  def show_page_from_form() = Action(parse.multipartFormData) { request =>
    val pageId = request.body.asFormUrlEncoded("pageId").map({ dsOneId => dsOneId.toString.trim }).head
    val pageNumb = request.body.asFormUrlEncoded("pageNumb").map({ dsOneId => dsOneId.toString.trim }).head
    Redirect(routes.DatasetController.show_page(pageId.toInt, pageNumb.toInt))
  }

  def integrate = Action(parse.multipartFormData) { request =>
    Logger.info("Calling integrations upload ...")

    val name = request.body.asFormUrlEncoded("integration_name").map({ dsOneId => dsOneId.toString.trim }).head

    val dsOneId = request.body.asFormUrlEncoded("dsOneId").map({ dsOneId => dsOneId.toString.trim }).head
    val dsOneName = db.getDatasetName(dsOneId.toInt)
    val dsOneFields = db.getDatasetFields(dsOneName).filter(field => field != "COLUMN_ID")
    val dsOne: Dataset = Dataset(dsOneId.toInt, dsOneName)
    dsOneFields.map(field => dsOne.addField(field))

    val dsTwoId = request.body.asFormUrlEncoded("dsTwoId").map({ dsTwoId => dsTwoId.toString.trim }).head
    val dsTwoName = db.getDatasetName(dsTwoId.toInt)
    val dsTwoFields = db.getDatasetFields(dsTwoName).filter(field => field != "COLUMN_ID")
    val dsTwo: Dataset = Dataset(dsTwoId.toInt, dsTwoName)
    dsTwoFields.map(field => dsTwo.addField(field))

    Ok(views.html.integrate(name, dsOne, dsTwo))
  }

}
