package models

import play.api.libs.json._
import java.util.{Map => JavaMap, Date}
import scala.collection.JavaConversions._
import scala.language.implicitConversions
import aws.dynamodb._
import com.amazonaws.services.dynamodbv2.model.AttributeValue

case class Widget(
	id:String = Value.guid,
  name:String,
  created:Date
)

object Widget extends ((String,String,Date) => Widget) with Model[Widget] {

  implicit val r = Json.reads[Widget]
  implicit val w = Json.writes[Widget]

  def nextId = Attribute.guid
  val table = "widgets"
  val keyName = "id"

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

  implicit def config = Config()

  def getById(id:Any) =
    getItem(id)

  def create(name:String) =
    putItem(Widget(Value.guid,name,new Date()))

  def update(id:Long,name:String) =
    updateItem(id,Map("name" -> Update.from(name)))

  def delete(id:Long) =
    deleteItem(id)

  def getRecent(lastModified:Date) =
    queryItems(Where.greaterThan("created",lastModified).conditions)

  def scan =
    scanItems()

  def getWithNameRecent(name:String) =
    queryItems(Where.equalTo("name",name).conditions)

}
