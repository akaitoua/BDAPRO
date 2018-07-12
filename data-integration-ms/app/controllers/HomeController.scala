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
class HomeController @Inject()(h2: H2Controller, cc: ControllerComponents, dsc: DatasetController) extends AbstractController(cc) {

  /**
   * Create an Action to render an HTML page with a welcome message.
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
  def index = Action {

    val datasets = h2.getDatasets()
    datasets.map(dataset => dataset.formatId())

    //Ok(views.html.index(files.map(getDataset(_))))
    Ok(views.html.index(datasets))
  }

}
