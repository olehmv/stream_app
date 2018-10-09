package app.utils.xml

import scala.xml.NodeSeq

class ExecuteQuery(_sqlFile: String, _checkPoint: CheckPoint) {

  def sql        = _sqlFile
  def checkPoint = _checkPoint

  def sql_        = _sqlFile
  def checkPoint_ = _checkPoint

  def toXML =
    <executequery sqlfile={_sqlFile}>
      {_checkPoint.toXML}
    </executequery>

}

object ExecuteQuery {

  def fromXML(node: NodeSeq) =
    new ExecuteQuery(
      _sqlFile = (node \ "@sqlfile") text,
      _checkPoint = CheckPoint.fromXML((node \ "checkpoint"))
    )

}
