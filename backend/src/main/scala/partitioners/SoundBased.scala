package partitioners

import org.apache.commons.codec.language._
class SoundBased extends Serializable {

  @transient lazy val metaphone = new Metaphone()

  def getSoundCode(col:String):String = if (col!="null") metaphone.encode(col) else col


}
