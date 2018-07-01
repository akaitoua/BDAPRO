package models

import play.api.libs.json.{Json, Writes}

case class Dataset(id: String, name: String) {}

object Dataset {
  implicit val datasetWrites = new Writes[Dataset] {
    def writes(dataset: Dataset) = Json.obj(
      "id" -> dataset.id,
      "name" -> dataset.name
    )
  }
}
