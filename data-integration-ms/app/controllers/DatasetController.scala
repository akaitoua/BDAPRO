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
class DatasetController  @Inject()(cc: ControllerComponents, db: DatasetDBController, fc: FilesController) extends AbstractController(cc){


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
  def download(id: String) = Action {
    Logger.info(s"Calling download action (id:$id) ...")
    val dataset = db.getDataset(id)

    if (dataset == null) NotFound(views.html.todo())
    else Ok.sendFile(new java.io.File(dataset.getFileName()))
  }

  def update(id: String) = Action(parse.multipartFormData) { request =>

    Logger.info("Calling update action ...")

    val currentDirectory = new java.io.File(".").getCanonicalPath
    var name = ""

    request.body.asFormUrlEncoded("datasetName").map( { datasetName =>
      name = datasetName.toString.trim
    })

    val filePath = s"$currentDirectory/datasets/$id-$name.csv"

    request.body.file("dataset").map { dataset =>


      val fileName = dataset.filename

      if(fileName == "") {
        Logger.info(s"File not found im form")
        val oldFile = fc.getListFile(id.toInt).head.getAbsolutePath
        fc.mv(oldFile, filePath)
        db.renameTable(id, name)

      } else {
        Logger.info(s"Uploading file: $fileName")
        fc.deleteFile(id)
        dataset.ref.moveTo(Paths.get(filePath), replace = true)
        Logger.info(s"File $id-$name.csv added!")

        val newFile = fc.getListFile(id.toInt).head
        db.update(id, db.createFromFile(newFile))
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
    val id = "%03d".format(files.length + 1)

    val name = request.body.asFormUrlEncoded("datasetName").map( { datasetName =>
        datasetName.toString.replace(" ", "_").trim
    }).head

    Logger.info("New dataset name: " + name)

    request.body.file("dataset").map { dataset =>

      val fileName = dataset.filename
      Logger.info(s"Uploading file: $fileName")

      dataset.ref.moveTo(Paths.get(s"/$currentDirectory/datasets/$id-$name.csv"), replace = true)
      Logger.info(s"File $id-$name.csv added!")

      Logger.info("Uploading dataset to H2 ...")
      val ds = db.createFromFile(new File(s"/$currentDirectory/datasets/$id-$name.csv"))
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
  def delete(id: String) = Action {

    //TODO: implement async deletion with Monads and Futures
    /*implicit class FileMonads(f: File) {
      def check = Future{ f.exists } //returns "Future" monad
      def remove = Future{ f.delete() } //returns "Future" monad
    }*/
    Logger.info(s"Calling delete action (id:$id) ...")

    if (fc.deleteFile(id)) {
      db.delete(id)
      Redirect(routes.DatasetController.index)
    }
    else NotFound(views.html.todo())
  }

  def show(id: String) = Action {

    val t = db.read(id)
    val ds : Dataset = t._1
    val rows = t._2
    val length = db.getDatasetSize(id)

    val pages =  if (length % 10 == 0) ((1 to (length / 10)).toArray) else ((1 to (length / 10) + 1).toArray)
    Ok(views.html.dataset(id, ds.displayName(), ds.fields, rows, 1, pages))
  }

  def show_page(id: String, pagNumb: Int) = Action {
    val from = 10 * pagNumb
    val to = from + 10
    val t = db.read(id, from, to)
    val ds : Dataset = t._1
    val rows = t._2
    val length = db.getDatasetSize(id)

    val pages =  if (length % 10 == 0) ((1 to (length / 10)).toArray) else ((1 to (length / 10) + 1).toArray)
    Ok(views.html.dataset(id, ds.displayName(), ds.fields, rows, pagNumb, pages))
  }

  def integrate = Action(parse.multipartFormData)  { request =>
    Logger.info("Calling integrations upload ...")
    val dsOneId = request.body.asFormUrlEncoded("dsOneId").map( { dsOneId => dsOneId.toString.trim}).head
    val dsOneName = db.getDatasetName("%03d".format(dsOneId.toInt))
    val dsOneFields = db.getDatasetFields(dsOneName).filter(field => field != "DATASET_ID")
    val dsOne : Dataset = Dataset(dsOneId, dsOneName)
    dsOneFields.map(field => dsOne.addField(field))

    val dsTwoId = request.body.asFormUrlEncoded("dsTwoId").map( { dsTwoId => dsTwoId.toString.trim}).head
    val dsTwoName = db.getDatasetName("%03d".format(dsTwoId.toInt))
    val dsTwoFields = db.getDatasetFields(dsTwoName).filter(field => field != "DATASET_ID")
    val dsTwo : Dataset = Dataset(dsTwoId, dsTwoName)
    dsTwoFields.map(field => dsTwo.addField(field))

    Ok(views.html.integrate(dsOne, dsTwo))
  }

}
