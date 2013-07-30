package aws.dynamodb

import java.nio.ByteBuffer
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import java.util.{HashMap,Map => JavaMap}

trait Field {

  def from(name:String,value:Any):JavaMap[String,AttributeValue] =
    value match {
      case value:String => S(name,value)
      case value:Long => N(name,value)
      case value:Int => N(name,value)
      case value:ByteBuffer => B(name,value)
      case _ => throw new Exception("Not a valid attribute type.")
    }

  def guid:JavaMap[String,AttributeValue] = Attribute.S("id",Value.guid)

  def N(name:String,value:Long):JavaMap[String,AttributeValue] = {
    val map:JavaMap[String,AttributeValue] = new HashMap[String,AttributeValue]()
    map.put(name,Value.N(value))
    map
  }

  def N(name:String,value:Int):JavaMap[String,AttributeValue] = {
    val map:JavaMap[String,AttributeValue] = new HashMap[String,AttributeValue]()
    map.put(name,Value.N(value))
    map
  }

  def B(name:String,value:ByteBuffer):JavaMap[String,AttributeValue] = {
    val map:JavaMap[String,AttributeValue] = new HashMap[String,AttributeValue]()
    map.put(name,Value.B(value))
    map
  }

  def S(name:String,value:String):JavaMap[String,AttributeValue] = {
    val map:JavaMap[String,AttributeValue] = new HashMap[String,AttributeValue]()
    map.put(name,Value.S(value))
    map
  }
}