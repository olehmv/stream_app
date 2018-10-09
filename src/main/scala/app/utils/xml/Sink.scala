package app.utils.xml

import scala.xml.NodeSeq

/**
  * Xml element from parameter.xml file
  * Used to hold Spark Sink options
  * @param _format
  * @param _outputMode
  * @param _options
  */
class Sink(_format: String, _outputMode: String, _options: List[Option], _executeQuery: ExecuteQuery) {

  def format       = _format
  def outputMode   = _outputMode
  def options      = _options
  def executeQuery = _executeQuery

  def format_       = _format
  def outputMode_   = _outputMode
  def options_      = _options
  def executeQuery_ = _executeQuery

  def toXML =
    <sink format={_format} outputMode ={_outputMode}>
      {_executeQuery.toXML}
      { for (elam <- _options) yield  elam.toXML }
     
    </sink>

}

object Sink {

  def fromXML(node: NodeSeq): Sink =
    new Sink(
      _format = (node \ "@format") text,
      _outputMode = (node \ "@outputMode") text,
      _options = for (elem <- (node \ "option") toList) yield Option.fromXML(elem),
      _executeQuery = ExecuteQuery.fromXML((node \ "executequery"))
    )

}
