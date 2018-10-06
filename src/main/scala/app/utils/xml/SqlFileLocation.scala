package app.utils.xml

import scala.xml.NodeSeq

/**
  * Xml element from parameter.xml file
  * Used to hold sql file url and table name used for transformation in this file
  *
  * @param _url
  */
class SqlFileLocation(_url: String, _tableName: String) {

  def url        = _url
  def tableName  = _tableName
  def url_       = _url
  def tableName_ = _tableName

  def toXML =
    <sqlfilelocation url={_url} tableName={_tableName}/>
}

object SqlFileLocation {

  def fromXML(node: NodeSeq): SqlFileLocation =
    new SqlFileLocation(
      (node \ "@url") text,
      (node \ "@tableName") text
    )
}
