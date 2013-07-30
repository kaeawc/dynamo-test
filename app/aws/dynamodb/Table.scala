package aws.dynamodb

import com.amazonaws.services.dynamodbv2.model._
import java.util.{Map => JavaMap}
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.language.postfixOps
import ExecutionContext.Implicits.global

trait Table[A] {

  def nextId:JavaMap[String,AttributeValue]
  val table:String
  val keyName:String
  val rangeName:String = ""

  def initializeTable(item:A,readers:Long,writers:Long)(implicit config:Config) = {

    var schema = List[KeySchemaElement](new KeySchemaElement()
      .withKeyType("HASH")
      .withAttributeName(keyName))

    if(!rangeName.isEmpty)
      schema ++ List[KeySchemaElement](new KeySchemaElement()
        .withKeyType("RANGE")
        .withAttributeName(rangeName))

    val attributes = item.getClass().getDeclaredFields.foldLeft(List[AttributeDefinition]()) {
      (a, b) =>
        b.setAccessible(true)
        a ++ List[AttributeDefinition](new AttributeDefinition().withAttributeName(b.getName()).withAttributeType("S"))
    }

    val throughput = new ProvisionedThroughput()
      .withReadCapacityUnits(readers)
      .withWriteCapacityUnits(writers)

    createTable(schema, attributes, throughput)
  }

  protected def createTable(
  	schema:List[KeySchemaElement],
  	attributes:List[AttributeDefinition],
  	throughput:ProvisionedThroughput,
    indices:List[LocalSecondaryIndex] = List[LocalSecondaryIndex]()
  )(implicit config:Config) = {

    var request = new CreateTableRequest()
      .withTableName(table)
      .withKeySchema(schema)
      .withAttributeDefinitions(attributes)
      .withProvisionedThroughput(throughput)

    if(indices.length > 0)
      request = request.withLocalSecondaryIndexes(indices)

    Future { config.client.createTable(request) }

  }

  def deleteTable(implicit config:Config) = {

    val request = new DeleteTableRequest() withTableName table

    Future { config.client.deleteTable(request) }

  }

  def updateTable(throughput:ProvisionedThroughput)(implicit config:Config) = {

    val request = new UpdateTableRequest() withTableName table withProvisionedThroughput throughput

    Future { config.client.updateTable(request) }

  }

  def listTables(startName:String, limit:Int)(implicit config:Config) = {

    val request = new ListTablesRequest() withExclusiveStartTableName startName withLimit limit

    Future { config.client.listTables(request) }

  }

  def describeTable(implicit config:Config) = {

    val request = new DescribeTableRequest() withTableName table

    Future { config.client.describeTable(request) }

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