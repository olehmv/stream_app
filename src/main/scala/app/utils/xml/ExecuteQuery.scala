package app.utils.xml

import scala.xml.NodeSeq

class ExecuteQuery(_sqlFile: String, _waterMark: (String, String)) {

  def sql       = _sqlFile
  def waterMark = _waterMark

  def sql_       = _sqlFile
  def waterMark_ = _waterMark

  def toXML =
    <executequery sqlfile={_sqlFile} watermark={_waterMark}/>

}

object ExecuteQuery {

  def fromXML(node: NodeSeq) = {

    val seq: NodeSeq = node \ "@watermark"
    new ExecuteQuery(
      _sqlFile = (node \ "@sqlfile") text,
      _waterMark = ???
    )
  }

}
