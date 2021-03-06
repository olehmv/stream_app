package app.utils.xml

import scala.xml.NodeSeq

/**
  *  Xml element from parameter.xml file
  * Used to hold Spark Source options
  * @param _format
  * @param _path
  * @param _options
  */
class Source(_format: String, _path: String, _options: List[Option], _sourceTable: String, _waterMark: WaterMark) {

  def format      = _format
  def path        = _path
  def options     = _options
  def sourceTable = _sourceTable
  def waterMark   = _waterMark

  def format_      = _format
  def path_        = _path
  def options_     = _options
  def sourceTable_ = _sourceTable
  def waterMark_   = _waterMark

  def toXML =
    <source format={_format} path ={_path} sourcetable={_sourceTable}>
      {_waterMark.toXML}
      { for (option <- _options) yield  option.toXML }
    </source>

}

object Source {

  def fromXML(node: NodeSeq): Source =
    new Source(
      _format = (node \ "@format") text,
      _path = (node \ "@path") text,
      _options = for (source <- (node \ "option") toList) yield Option.fromXML(source),
      _sourceTable = (node \ "@sourcetable") text,
      _waterMark = WaterMark.fromXML((node \ "watermark"))
    )

}
