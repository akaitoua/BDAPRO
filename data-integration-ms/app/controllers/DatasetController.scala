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
class DatasetController  @Inject()(cc: ControllerComponents) extends AbstractController(cc){

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

      deleteFile(id)

      val fileName = dataset.filename
      Logger.info(s"Uploading file: $fileName")

      dataset.ref.moveTo(Paths.get(filePath), replace = true)
      Logger.info(s"File $id-$name.csv added!")

      Redirect(routes.HomeController.index)
    }.getOrElse {
      Logger.info(s"File not found im form")
      val oldFile = getListFile(new File(s"/$currentDirectory/datasets/"), id.toInt).head.getAbsolutePath
      mv(oldFile, filePath)
      Redirect(routes.HomeController.index)
      //Ok(views.html.todo())
    }
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

      Logger.info("Uploading dataset to H2 ...")
      uploadToDB(new File(s"/$currentDirectory/datasets/$id-$name.csv"))
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

    if (fileDeleted) Redirect(routes.HomeController.index)
    else NotFound(views.html.todo())
  }

  def show(id: String) = Action {

    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement
    val x = "%03d".format(id.toInt)
    var columnNames = Array[String]()
    var rows = Array[Array[String]]()
    var lenght = 0

    try{
      var name = ""
      val rs = stmt.executeQuery(s"SELECT name FROM DATASET WHERE id=$x")
      if (rs.next()) name = rs.getString("name")

      name = name.toUpperCase()
      val str = s"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '$name'"



      val rs2 = stmt.executeQuery(str)
      while (rs2.next()){
        columnNames = columnNames :+ rs2.getString("COLUMN_NAME")
      }

      val rs4 = stmt.executeQuery(s"SELECT COUNT(*) as RESULT FROM $name;")
      if (rs4.next()) lenght = rs4.getInt("RESULT")
      println(lenght)


      val rs3 = stmt.executeQuery(s"SELECT * FROM $name WHERE DATASET_ID < 11")
      while (rs3.next()){
        var row = ""
        for (columnName <- columnNames){
          row += rs3.getString(columnName) + "\t"
        }
        rows = rows :+ row.split("\t").drop(1)
      }
    }
    val pages =  if (lenght % 10 == 0) ((1 to (lenght / 10)).toArray) else ((1 to (lenght / 10) + 1).toArray)
    Ok(views.html.dataset(columnNames.drop(1), rows, 1, pages))
  }

  def show_page(id: String, pagNumb: Int) = Action {
    /*val currentDirectory = new java.io.File(".").getCanonicalPath
    val dir = new File(s"/$currentDirectory/datasets/")

    val files = getListFile(dir, id.toInt)
    val file = files.head
    val bufferedSource = Source.fromFile(file.getAbsolutePath)
    var rows = Array[Array[String]]()

    for(line <- bufferedSource.getLines()){
      rows = rows :+ line.split("\t")
    }
    val x = pagNumb - 1
    val headers = rows.apply(0)
    val dataRows = rows.drop(1).slice(x * 10,(x+1) * 10)
    val pages = (1 to (rows.length / 10)).toArray
    Ok(views.html.dataset(headers, dataRows,pagNumb, pages)) */

    println(id)

    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement
    val x = "%03d".format(id.toInt)
    var columnNames = Array[String]()
    var rows = Array[Array[String]]()
    var lenght = 0

    try{
      var name = ""
      val rs = stmt.executeQuery(s"SELECT name FROM DATASET WHERE id=$x")
      if (rs.next()) name = rs.getString("name")

      name = name.toUpperCase()
      val str = s"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '$name'"



      val rs2 = stmt.executeQuery(str)
      while (rs2.next()){
        columnNames = columnNames :+ rs2.getString("COLUMN_NAME")
      }

      val rs4 = stmt.executeQuery(s"SELECT COUNT(*) as RESULT FROM $name;")
      if (rs4.next()) lenght = rs4.getInt("RESULT")
      println(lenght)

      val from = (pagNumb - 1) * 10
      val to = pagNumb * 10
      println(s"FROM $from TO $to")
      val rs3 = stmt.executeQuery(s"SELECT * FROM $name WHERE DATASET_ID > $from AND DATASET_ID < $to")
      while (rs3.next()){
        var row = ""
        for (columnName <- columnNames){
          row += rs3.getString(columnName) + "\t"
        }
        rows = rows :+ row.split("\t").drop(1)
      }
    }
    val pages =  if (lenght % 10 == 0) ((1 to (lenght / 10)).toArray) else ((1 to (lenght / 10) + 1).toArray)
    Ok(views.html.dataset(columnNames.drop(1), rows, pagNumb, pages))
  }

  def uploadToDB(file: File) = {

    var rows : Array[String] = Array[String]()
    val bufferedSource = Source.fromFile(file.getAbsolutePath)
    for(line <- bufferedSource.getLines()){
      rows = rows :+ line
    }
    val args = file.getName.split("-")
    val id = args(0)
    val name = args(1).replace(".csv","")
    val headers = rows.apply(0).split("\t")
    val data = rows.drop(1)

    createTable(name, headers)
    uploadData(id, name, headers, data)
  }

  def uploadData(id: String, name: String, headers: Array[String], data: Array[String]) = {

    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement

    var colNames = "DATASET_ID, "
    for (header <- headers){ colNames += header + ',' }
    colNames = colNames.dropRight(1)

    try {
      stmt.execute(s"INSERT INTO DATASET (id, name) VALUES ('$id', '$name') ;")
      var count = 1
      for(line <- data){

        var values = s"$count,"

        for(v <- line.split("\t")){
          if(v != "") values += "\'" + v.replace("'", "''") + "\',"
          else values += "\'none\',"
        }
        values = values.dropRight(1)
        stmt.execute(s"INSERT INTO $name ($colNames) VALUES ($values);")
        count += 1;
      }

    } finally {
      conn.close()
      stmt.close()
    }



  }

  def createTable(name: String, headers: Array[String]) = {

    var cols = "DATASET_ID INT PRIMARY KEY, "
    for(header <- headers){ cols += s"$header VARCHAR(255)," }
    cols = cols.dropRight(1)

    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement

    try{
      stmt.execute(s" DROP TABLE $name")
    }catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    }

    try {
      val stmt = conn.createStatement
      stmt.execute(s"CREATE TABLE $name ($cols);")
    }
    catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    }finally {
      conn.close()
    }

  }

  def dropTable(name: String) = {

    val conn = Databases.inMemory().getConnection()
    val stmt = conn.createStatement

    try{
      stmt.execute(s"DROP TABLE $name;")
      stmt.execute(s"DELETE FROM DATASET WHERE $name = name;")
    }catch {
      case e: JdbcSQLException => Logger.info(e.getMessage)
    } finally {
      stmt.close()
      conn.close()
    }

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
      dropTable(name)
      if (file.delete()) deleted = true
    }
    deleted
  }

  def mv(oldName: String, newName: String) =
    Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)

}
