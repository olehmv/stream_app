package app.utils.xml

import scala.xml.NodeSeq

/**
  * Xml element from parameter.xml file
  * Used to hold parameters to Spark Job
  * Source from where to read
  * Sink where to write
  * List of columns that needs to be transform
  * @param _source
  * @param _sink
  * @param _transform
  */
class Parameter(_source: Source,
                _sink: Sink,
                _transform: List[Transform],
                _sqlFileLocation: SqlFileLocation) {
  def source          = _source
  def sink            = _sink
  def transform       = _transform
  def sqlFileLocation = _sqlFileLocation

  def source_          = _source
  def sink_            = _sink
  def transform_       = _transform
  def sqlFileLocation_ = _sqlFileLocation

  def toXML =
    <parameter>
      {source.toXML}
      {sink.toXML}
      {for(elem<-_transform)yield elem.toXML}
      {sqlFileLocation.toXML}
    </parameter>

}

object Parameter {

  def fromXML(node: NodeSeq): Parameter =
    new Parameter(
      _source = Source.fromXML(node \ "source"),
      _sink = Sink.fromXML(node \ "sink"),
      _transform = for (elem <- (node \ "transform").toList)
        yield Transform.fromXML(elem),
      _sqlFileLocation = SqlFileLocation.fromXML(node \ "sqlfilelocation")
    )

}
