package controllers

import java.io.File
import java.nio.file.Paths

import javax.inject._
import play.api.Logger
import play.api.libs.json.Json
import play.api.mvc._
import play.api.libs.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


@Singleton
class DatasetController  @Inject()(cc: ControllerComponents) extends AbstractController(cc){

  /* Classes and implicit values to generate the Dataset json: */

  case class Dataset(id: Int, name: String)
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
      id == dsId.toInt
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
      val name = datasetArgs.apply(1)
      dsSeq = dsSeq :+ Dataset(id.toInt, name)
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

  def update(id: String) = Action {Ok(views.html.todo())}

  /*
  Uploads a new dataset file
  Responsible for the POST /api/dataset route
   */
  def upload = Action(parse.multipartFormData) { request =>

    Logger.info("Calling upload action ...")

    val currentDirectory = new java.io.File(".").getCanonicalPath
    val okFileExtensions = List("csv")
    val files = getListOfFiles(new File(s"/$currentDirectory/datasets/"), okFileExtensions)
    var name = ""
    val id = "%03d".format(files.length + 1)

    request.body.asFormUrlEncoded("datasetName").map( { datasetName =>
        name = datasetName.toString.trim
    })

    Logger.info("New dataset name: " + name)

    request.body.file("dataset").map { dataset =>

      val fileName = dataset.filename
      Logger.info(s"Uploading file: $fileName")

      dataset.ref.moveTo(Paths.get(s"/$currentDirectory/datasets/$id-$name.csv"), replace = true)
      Logger.info(s"File $id-$name.csv added!")

      Ok(views.html.index())
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

    val currentDirectory = new java.io.File(".").getCanonicalPath
    val dir = new File(s"/$currentDirectory/datasets/")

    val files = getListFile(dir, id.toInt)
    var deleted = false

    for (file <- files){
      val fileName = file.getName
      Logger.info(s"Found file with id=$id\n -> $fileName")
      if (file.delete()) deleted = true
    }

    if (deleted) Ok(views.html.todo())
    else NotFound(views.html.todo())
  }

}
