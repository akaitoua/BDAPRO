package controllers

import java.io.File

import javax.inject._
import models.Dataset
import play.api.mvc._
import play.api.Logger

import scala.collection.mutable.ListBuffer

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject()(cc: ControllerComponents, dsc: DatasetController) extends AbstractController(cc) {

  /**
   * Create an Action to render an HTML page with a welcome message.
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
  def index = Action {

    val currentDirectory = new java.io.File(".").getCanonicalPath
    val okFileExtensions = List("csv")
    val files = dsc.getListOfFiles(new File(s"/$currentDirectory/datasets/"), okFileExtensions)

    def getDataset(file: File): Dataset = {
      val fileName = file.getName
      val datasetArgs = fileName.split("-")
      val id = datasetArgs.apply(0)
      val name = datasetArgs.apply(1)
      Dataset(id, name)
    }

    Ok(views.html.index(files.map(getDataset(_))))
  }

}
