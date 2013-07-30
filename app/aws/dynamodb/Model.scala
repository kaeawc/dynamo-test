package aws.dynamodb

import java.util.{HashMap,Map => JavaMap}
import com.amazonaws.services.dynamodbv2.model._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.language.postfixOps
import ExecutionContext.Implicits.global

trait Model[A] {

  def nextId:JavaMap[String,AttributeValue]
  val table:String
  val keyName:String

  def logConsumed(consumed:ConsumedCapacity) {}

  def logMetrics(metrics:ItemCollectionMetrics) {}

  def getItem(id:Any,attributes:List[String] = Nil)(implicit config:Config): Future[Option[A]] = {

    val key = Key from("id", id)
    var request = new GetItemRequest() withTableName table withKey key

    if(attributes.length > 0)
      request = request withAttributesToGet attributes

    val result = Future { config.client.getItem(request) }

    result.map {
      get =>
        logConsumed(get.getConsumedCapacity)
        readDynamo(get.getItem)
    }
  }

  def putItem(item:JavaMap[String,AttributeValue])(implicit config:Config): Future[Option[A]] = {

    val request = new PutItemRequest() withTableName table withItem item
    val result = Future { config.client.putItem(request) }

    result.map {
      put =>
        logConsumed(put.getConsumedCapacity)
        logMetrics(put.getItemCollectionMetrics)
        readDynamo(put.getAttributes)
    }
  }

  def deleteItem(id:Any)(implicit config:Config): Future[Option[A]] = {

    val key = Key from("id", id)
    val request = new DeleteItemRequest() withTableName table withKey key
    val result = Future { config.client.deleteItem(request) }

    result.map {
      deleted =>
        logConsumed(deleted.getConsumedCapacity)
        logMetrics(deleted.getItemCollectionMetrics)
        readDynamo(deleted.getAttributes)
    }
  }

  def updateItem(id:Any,attributes:JavaMap[String,AttributeValueUpdate])(implicit config:Config): Future[Option[A]] = {

    val key = Key from("id", id)
    var request = new UpdateItemRequest() withTableName table withKey key withAttributeUpdates attributes
    val result = Future { config.client.updateItem(request) }

    result.map {
      updated =>
        logConsumed(updated.getConsumedCapacity)
        logMetrics(updated.getItemCollectionMetrics)
        readDynamo(updated.getAttributes)
    }
  }

  def scanItems(attributes:List[String] = Nil,startKey:AttributeValue = null)(implicit config:Config): Future[List[A]] = {

    var request = new ScanRequest() withTableName table withAttributesToGet attributes
    val result = Future { config.client.scan(request) }

    result.map {
      scanned =>
        logConsumed(scanned.getConsumedCapacity)

        lazy val next = Await.result(scanItems(attributes, scanned.getLastEvaluatedKey.get(keyName)), 1 second)

        scanned.getItems.foldLeft(List[A]()) {
          (a,b) => a ++ readDynamo(b)
        } ++ next
    }
  }

  def queryItems(conditions:JavaMap[String,Condition] = new HashMap[String,Condition],attributes:List[String] = Nil,limit:Int = 100,startKey:AttributeValue = null)(implicit config:Config): Future[List[A]] = {

    var request = new QueryRequest().withTableName(table)

    for (condition <- conditions) {
      val (key,value) = condition
      value.getComparisonOperator match {
      	case "NE"           => throw new Exception("Cannot query by NE on " + key + ", condition operator is not supported.")
      	case "NOT_NULL"     => throw new Exception("Cannot query by NOT_NULL on " + key + ", condition operator is not supported.")
      	case "NULL"         => throw new Exception("Cannot query by NULL on " + key + ", condition operator is not supported.")
      	case "CONTAINS"     => throw new Exception("Cannot query by CONTAINS on " + key + ", condition operator is not supported.")
      	case "NOT_CONTAINS" => throw new Exception("Cannot query by NOT_CONTAINS on " + key + ", condition operator is not supported.")
      	case "IN"           => throw new Exception("Cannot query by IN on " + key + ", condition operator is not supported.")
      	case _ => {}
      }
    }

    if(!conditions.isEmpty)
      request = request.withKeyConditions(conditions)


    if(!attributes.isEmpty)
      request = request.withAttributesToGet(attributes)

    if(limit > 0)
      request = request.withLimit(limit)

    if(startKey != null)
      request = request.withExclusiveStartKey(Map(keyName -> startKey))

    val result = Future { config.client.query(request) }

    result.map {
      list =>
        logConsumed(list.getConsumedCapacity)

        lazy val next = Await.result(queryItems(conditions, attributes, limit, list.getLastEvaluatedKey.get(keyName)), 1 second)

        list.getItems.foldLeft(List[A]()) {
          (a,b) => a ++ readDynamo(b)
        } ++ next
    }
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