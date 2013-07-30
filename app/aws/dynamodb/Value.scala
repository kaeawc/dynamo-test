package aws.dynamodb

import java.nio.ByteBuffer
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import java.util.{Date,HashMap,Map => JavaMap}
import java.text.SimpleDateFormat

object Value {

  val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  def from(value:Any):AttributeValue =
    value match {
      case value:Date => S(df.format(value))
      case value:String => S(value)
      case value:Long => N(value)
      case value:Int => N(value)
      case value:ByteBuffer => B(value)
      case _ => throw new Exception("Not a valid attribute type.")
    }

  def N(value:Long) = new AttributeValue().withN(value.toString)
  def N(value:Int) = new AttributeValue().withN(value.toString)
  def B(value:ByteBuffer) = new AttributeValue().withB(value)
  def S(value:String) = new AttributeValue().withS(value)

  def guid:String = "asdf"
}
