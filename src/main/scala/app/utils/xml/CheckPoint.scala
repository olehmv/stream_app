package app.utils.xml

import scala.xml.NodeSeq

class CheckPoint(_key: String, _value: String) {

  def key   = _key
  def value = _value

  def key_   = _key
  def value_ = _value

  def toXML =
    <checkpoint key={_key} value={_value}/>

}

object CheckPoint {

  def fromXML(node: NodeSeq) =
    new CheckPoint(
      _key = (node \ "@key") text,
      _value = (node \ "@value") text
    )
}
