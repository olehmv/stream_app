package app.utils.xml

import scala.xml.NodeSeq

/**
  * Xml element from parameter.xml file
  * Used to hold parameters to Spark Job
  * Source from where to read
  * List of Sinks where to write
  * List of columns that needs to be transform
  * @param _source
  * @param _sinks
  * @param _transformColumns
  */
class Parameter(_source: Source, _sinks: List[Sink], _transformColumns: List[TransformColumnName]) {
  def source    = _source
  def sink      = _sinks
  def transform = _transformColumns

  def source_    = _source
  def sink_      = _sinks
  def transform_ = _transformColumns

  def toXML =
    <parameter>
      {source.toXML}
      {for(elem<-_transformColumns)yield elem.toXML}
      {for(elem<-_sinks)yield elem.toXML}
    </parameter>

}

object Parameter {

  def fromXML(node: NodeSeq): Parameter =
    new Parameter(
      _source = Source.fromXML(node \ "source"),
      _sinks = for (elem            <- (node \ "sink") toList) yield Sink.fromXML(elem),
      _transformColumns = for (elem <- (node \ "transformcolumnname").toList)
        yield TransformColumnName.fromXML(elem)
    )

}
