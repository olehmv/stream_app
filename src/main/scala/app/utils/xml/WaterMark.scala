package app.utils.xml

import scala.xml.NodeSeq

class WaterMark(_columnName: String, _timeInterval: String) {

  def columnName   = _columnName
  def timeInterval = _timeInterval

  def toXML =
    <watermark columnname={_columnName} timeInterval={_timeInterval}/>

}

object WaterMark {

  def fromXML(node: NodeSeq) =
    new WaterMark(
      _columnName = (node \ "@columnname") text,
      _timeInterval = (node \ "@timeInterval") text
    )

}
