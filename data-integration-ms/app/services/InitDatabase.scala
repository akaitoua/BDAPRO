package services

import java.time.Clock

import controllers.{DatasetDBController, FilesController}
import javax.inject.{Inject, Singleton}
import play.api.db._

@Singleton
class InitDatabase @Inject() (db: Database, dsDBController: DatasetDBController, fc: FilesController){

  val files = fc.getFiles()
  files.foreach( file => {
    val ds = fc.createDataset(file)
    dsDBController.add(ds)
  })

}
