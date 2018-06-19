package controllers

import java.io.File
import java.nio.file.Paths

import javax.inject._
import play.Play
import play.api.Logger
import play.api.data.Form
import play.api.libs.json.Json
import play.api.libs.json.JsonNaming.Identity
import play.api.mvc._

import services.Counter
import play.api.libs.json._

@Singleton
class DatasetController  @Inject()(cc: ControllerComponents) extends AbstractController(cc){

  case class Dataset(id: Int, name: String)
  case class Summary(datasets: Seq[Dataset])

  implicit val datasetWrites = new Writes[Dataset] {
    def writes(dataset: Dataset) = Json.obj(
      "id" -> dataset.id,
      "name" -> dataset.name
    )
  }

  implicit val summaryWrites = new Writes[Summary] {
    def writes(summary: Summary) = Json.obj(
      "datasets" -> summary.datasets
    )
  }

  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

  def index = Action {

    val appPath = Play.application().path()
    val okFileExtensions = List("csv")
    val files = getListOfFiles(new File(s"/$appPath/datasets/"), okFileExtensions)

    var dsSeq = Seq[Dataset]()

    for (file <- files){
      val fileName = file.getName
      val datasetArgs = fileName.split("-")

      val id = datasetArgs.apply(0)
      val name = datasetArgs.apply(1)
      dsSeq = dsSeq :+ Dataset(id.toInt, name)
    }
    //Logger.info(files.toString())

    val values = Summary(dsSeq)
    val json = Json.toJson(values)

    Ok(Json.prettyPrint(json))
  }

  def read(id: String) = Action {Ok(views.html.todo())}

  def update(id: String) = Action {Ok(views.html.todo())}

  def upload = Action(parse.multipartFormData) { request =>

    var appPath = Play.application().path()
    val okFileExtensions = List("csv")
    val files = getListOfFiles(new File(s"/$appPath/datasets/"), okFileExtensions)
    var name = ""
    var id = "%03d".format(files.length + 1)

    request.body.asFormUrlEncoded("datasetName").map( { datasetName =>
        name = datasetName.toString.trim
    })


    request.body.file("dataset").map { dataset =>
      // only get the last part of the filename
      // otherwise someone can send a path like ../../home/foo/bar.txt to write to other files on the system
      val filename = Paths.get(dataset.filename).getFileName
      var appPath = Play.application().path()

      dataset.ref.moveTo(Paths.get(s"/$appPath/datasets/$id-$name.csv"), replace = true)
      Logger.info(s"File $id-$name.csv added!")
      Ok(views.html.index())
    }.getOrElse {
      Logger.error(s"File not found im form")
      Redirect(routes.HomeController.index).flashing(
        "error" -> "Missing file")
    }


  }

  def delete(id: String) = Action {Ok(views.html.todo())}



}
