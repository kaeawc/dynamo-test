package models

import play.api.libs.json._

import com.amazonaws.services.dynamodbv2.model._
import java.util.{Map => JavaMap, Date}
import scala.collection.JavaConversions._
import aws.dynamodb._

case class Widget(
	id:String = Value.guid,
  name:String,
  created:Date
)

object Widget extends ((String,String,Date) => Widget) with Model[Widget] {

  implicit val r = Json.reads[Widget]
  implicit val w = Json.writes[Widget]

  def nextId:JavaMap[String,AttributeValue] = Attribute.guid
  val table:String = "widgets"

  implicit def readDynamo(values: JavaMap[String, AttributeValue]):Option[Widget] = {
    try {
      val id = values.get("id").asInstanceOf[String]
      val name = values.get("name").asInstanceOf[String]
      val created = values.get("created").asInstanceOf[Date]
      Some(Widget(id,name,created))
    } catch {
      case e:Exception => None
    }
  }

  def create(name:String) = putItem(Widget(Value.guid,name,new Date()))

  def update(id:Long,name:String) = updateItem(id,Map("name" -> Update.S(name)))

  def delete(id:Long) = deleteItem(id)

  def getRecent(lastModified:Date) = queryItem(Where.greaterThan("created",lastModified).conditions)

  def getWithNameRecent(name:String) = queryItem(Where.equalTo("name",name).conditions)

}
