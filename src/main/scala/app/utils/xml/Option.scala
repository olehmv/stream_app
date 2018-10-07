package app.utils.xml

/**
  * Xml element from parameter.xml file
  * Used to hold option parameter to Spark job
  * @param _key
  * @param _value
  */
class Option(_key: String, _value: String) {

  def key   = _key
  def value = _value

  def key_   = _key
  def value_ = _value

  def toXML =
      <option key={_key} value={_value}/>

}

object Option {

  def fromXML(node: scala.xml.NodeSeq): Option =
    new Option(
      _key = (node \ "@key") text,
      _value = (node \ "@value") text
    )

}
