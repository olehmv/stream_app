package app.utils.xml

import scala.xml.NodeSeq

/**
  * Xml element from parameter.xml file
  * Used to hold timestamp pattern and columns that needs to be transform from this pattern
  * @param _pattern
  * @param _columns
  */
class TransformColumns(_pattern: String, _columns: List[Column]) {

  def pattern = _pattern
  def columns = _columns

  def pattern_ = _pattern
  def columns_ = _columns

  def toXML =
    <transform pattern={_pattern}>
      {for(column <- _columns) yield column.toXML}
    </transform>

}

object TransformColumns {

  def fromXML(node: NodeSeq) =
    new TransformColumns(
      _pattern = (node \ "@pattern") text,
      _columns = for (column <- (node \ "column") toList) yield Column.fromXML(column)
    )
}
