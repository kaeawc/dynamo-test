package aws.dynamodb

import java.nio.ByteBuffer
import com.amazonaws.services.dynamodbv2.model.{ComparisonOperator, Condition, AttributeValueUpdate}
import java.util.{Date,HashMap,Map => JavaMap}
import java.text.SimpleDateFormat

case class Update(attributes:JavaMap[String, AttributeValueUpdate] = new HashMap[String,AttributeValueUpdate]()) {

  def and(name:String,newAttribute:AttributeValueUpdate):Update = {
    attributes.put(name,newAttribute)
    this
  }


}

object Update {

  val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  def from(value:Any):AttributeValueUpdate =
    value match {
      case value:Date => S(df.format(value))
      case value:String => S(value)
      case value:Long => N(value)
      case value:Int => N(value)
      case value:ByteBuffer => B(value)
      case _ => throw new Exception("Not a valid attribute type.")
    }

  def N(value:Long) = new AttributeValueUpdate().withValue(Value.N(value))
  def N(value:Int) = new AttributeValueUpdate().withValue(Value.N(value))
  def B(value:ByteBuffer) = new AttributeValueUpdate().withValue(Value.B(value))
  def S(value:String) = new AttributeValueUpdate().withValue(Value.S(value))

  def guid:String = "asdf"
}
