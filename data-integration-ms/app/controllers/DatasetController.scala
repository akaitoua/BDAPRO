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
class DatasetController  @Inject()(cc: ControllerComponents, h2: H2Controller) extends AbstractController(cc){

  /* Classes and implicit values to generate the Dataset json: */
  case class DatasetCollection(datasets: Seq[Dataset])

  implicit val datasetWrites = new Writes[Dataset] {
    def writes(dataset: Dataset) = Json.obj(
      "id" -> dataset.id,
      "name" -> dataset.name
    )
  }

  implicit val summaryWrites = new Writes[DatasetCollection] {
    def writes(summary: DatasetCollection) = Json.obj(
      "datasets" -> summary.datasets
    )
  }

  /*
  Gets the list  of files with a list of extensions at a directory
   */
  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

  /*
  Get a files with an specific id
   */
  def getListFile(dir: File, id: Int): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      val fileName =file.getName()
      val args = fileName.split("-")
      val dsId = args.apply(0)
      if (args.length == 1) false
      else id == dsId.toInt
    }
  }

  /*
  Returns a json files with the information of all existing dataset files
  Responsible for the GET /api/datasets route
   */
  def index = Action {
    Logger.info("Calling index action ...")
    val currentDirectory = new java.io.File(".").getCanonicalPath
    val okFileExtensions = List("csv")
    val files = getListOfFiles(new File(s"/$currentDirectory/datasets/"), okFileExtensions)

    var dsSeq = Seq[Dataset]()

    for (file <- files){
      val fileName = file.getName
      Logger.info("Found file: " + fileName)

      val datasetArgs = fileName.split("-")
      val id = datasetArgs.apply(0)
      val name = datasetArgs.apply(1).replace(".csv", "")

      dsSeq = dsSeq :+ Dataset(id, name)
    }

    val values = DatasetCollection(dsSeq)
    val json = Json.toJson(values)
    Logger.info("End index action")
    Ok(Json.prettyPrint(json))
  }

  /*
  Gets an specific dataset file by id
  Responsible for the GET /api/dataset/:id route
   */
  def read(id: String) = Action {

    Logger.info(s"Calling read action (id:$id) ...")

    val currentDirectory = new java.io.File(".").getCanonicalPath
    val dir = new File(s"/$currentDirectory/datasets/")

    val files = getListFile(dir, id.toInt)
    Logger.info(s"Founded files for id=$id\n -> $files")
    if (files.length < 1) NotFound(views.html.todo())
    else Ok.sendFile(new java.io.File(dir + "/" + files.head.getName))

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
        val oldFile = getListFile(new File(s"/$currentDirectory/datasets/"), id.toInt).head.getAbsolutePath
        mv(oldFile, filePath)
        h2.renameTable(id, name)

      } else {
        Logger.info(s"Uploading file: $fileName")
        deleteFile(id)
        dataset.ref.moveTo(Paths.get(filePath), replace = true)
        Logger.info(s"File $id-$name.csv added!")

        val oldName = h2.getDatasetName(id)
        h2.dropTable(oldName)

        Logger.info("Uploading dataset to H2 ...")
        h2.uploadToDB(new File(s"/$currentDirectory/datasets/$id-$name.csv"))
        Logger.info("Dataset uploaded to H2")
      }

    }

    Redirect(routes.HomeController.index)
  }

  /*
  Uploads a new dataset file
  Responsible for the POST /api/dataset route
   */
  def upload = Action(parse.multipartFormData) { request =>

    Logger.info("Calling upload action ...")

    val currentDirectory = new java.io.File(".").getCanonicalPath
    val okFileExtensions = List("csv")
    val files = getListOfFiles(new File(s"/$currentDirectory/datasets/"), okFileExtensions)
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
      h2.uploadToDB(new File(s"/$currentDirectory/datasets/$id-$name.csv"))
      Logger.info("Dataset uploaded to H2")

      Redirect(routes.HomeController.index)
    }.getOrElse {
      Logger.error(s"File not found im form")
      Redirect(routes.HomeController.index).flashing(
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
    var fileDeleted = deleteFile(id)

    if (fileDeleted) {
      val dsId = "%03d".format(id.toInt)
      val name = h2.getDatasetName(dsId)
      h2.dropTable(name)
      Redirect(routes.HomeController.index)
    }
    else NotFound(views.html.todo())
  }

  def show(id: String) = Action {
    val name = h2.getDatasetName("%03d".format(id.toInt))
    val displayName = name.replace("_", " ").capitalize
    val columnNames = h2.getDatasetHeaders(name)
    val length = h2.getDatasetLength(name)
    val rows = h2.getDatasetContent(name, columnNames)
    val pages =  if (length % 10 == 0) ((1 to (length / 10)).toArray) else ((1 to (length / 10) + 1).toArray)
    Ok(views.html.dataset(id, displayName, columnNames.drop(1), rows, 1, pages))
  }

  def show_page(id: String, pagNumb: Int) = Action {
    val name = h2.getDatasetName("%03d".format(id.toInt))
    val displayName = name.replace("_", " ").capitalize
    val columnNames = h2.getDatasetHeaders(name)
    val length = h2.getDatasetLength(name)
    val rows = h2.getDatasetContent(name, columnNames)
    val pages =  if (length % 10 == 0) ((1 to (length / 10)).toArray) else ((1 to (length / 10) + 1).toArray)
    Ok(views.html.dataset(id, displayName, columnNames.drop(1), rows, pagNumb, pages))
  }


  def deleteFile(id: String): Boolean = {

    val currentDirectory = new java.io.File(".").getCanonicalPath
    val dir = new File(s"/$currentDirectory/datasets/")

    val files = getListFile(dir, id.toInt)
    var deleted = false

    for (file <- files){
      val fileName = file.getName
      Logger.info(s"Found file with id=$id\n -> $fileName")
      val name = fileName.split("-")(1).replace(".csv","")
      if (file.delete()) deleted = true
    }
    deleted
  }

  def mv(oldName: String, newName: String) =
    Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)

}
