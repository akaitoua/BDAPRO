package models

import play.api.libs.json.{Json, Writes}

case class Dataset(var id: String, var name: String) {
  def formatId() = {
    this.id = "%03d".format(id.toInt)
  }
}

