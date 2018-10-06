package app.utils.xml

import scala.xml.NodeSeq

/**
  * Xml element from parameter.xml file
  * Used to hold column to transform used in app.utils.Utils.transformStringColumnsToTimestampColumns method
  * from old string column name to new string column name
  * @param _from
  * @param _to
  */
class Column(_from: String, _to: String) {

  def from = _from
  def to   = _to

  def from_ = _from
  def to_   = _to

  def toXML =
    <column from={_from} to={_to}/>
}

object Column {

  def fromXML(node: NodeSeq) =
    new Column(
      _from = (node \ "@from") text,
      _to = (node \ "@to") text
    )

}
