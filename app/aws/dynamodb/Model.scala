package aws.dynamodb

import java.util.{HashMap,Map => JavaMap}
import scala.collection.JavaConversions._
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.model.AttributeValue

trait Model[A] {

  def nextId:JavaMap[String,AttributeValue]
  val table:String

  def getById(id:_):Option[A] =
    getItem(id).getItem

  def getItem(id:_,attributes:List[String] = Nil)(implicit config:Config): GetItemResult = {

    val key = Key from("id", id)
    val request = new GetItemRequest() withTableName table withKey key

    if(attributes.length > 0)
      config.client.getItem(request.withAttributesToGet(attributes))
    else
      config.client.getItem(request)
  }

  def putItem(item:JavaMap[String,AttributeValue])(implicit config:Config): PutItemResult = {

    var request = new PutItemRequest() withTableName table withItem item

    config.client.putItem(request)
  }

  def deleteItem(id:_)(implicit config:Config): DeleteItemResult = {

    val key = Key from("id", id)
    var request = new DeleteItemRequest() withTableName table withKey key

    config.client.deleteItem(request)
  }

  def updateItem(id:_,attributes:JavaMap[String,AttributeValueUpdate])(implicit config:Config): UpdateItemResult = {

    val key = Key from("id", id)
    var request = new UpdateItemRequest() withTableName table withKey key withAttributeUpdates attributes

    config.client.updateItem(request)
  }

  def queryItem(conditions:JavaMap[String,Condition] = new HashMap[String,Condition],attributes:List[String] = Nil,limit:Int = 100,startKey:String = null)(implicit config:Config): QueryResult = {

    var request = new QueryRequest().withTableName(table)

    if(!conditions.isEmpty)
      request = request.withKeyConditions(conditions)


    if(!attributes.isEmpty)
      request = request.withAttributesToGet(attributes)

    if(limit > 0)
      request = request.withLimit(limit)

    if(startKey != null)
      request = request.withExclusiveStartKey(Attribute.S("id",startKey))

    config.client.query(request)
  }

  implicit def readDynamo(attributes: JavaMap[String, AttributeValue]):Option[A]

  implicit def writeDynamo(item:A):JavaMap[String, AttributeValue] = {
    (Map[String, AttributeValue]() /: item.getClass.getDeclaredFields) {
      (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> Value.from(f.get(item)))
    }
  }

}