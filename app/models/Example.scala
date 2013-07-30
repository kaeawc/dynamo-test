import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Arrays
import java.util.Date
import java.util.{HashMap, Map => JavaMap}
import scala.collection.JavaConversions._

import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator
import com.amazonaws.services.dynamodbv2.model.Condition
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.amazonaws.services.dynamodbv2.model.GetItemResult
import com.amazonaws.services.dynamodbv2.model.QueryRequest
import com.amazonaws.services.dynamodbv2.model.QueryResult

object Example {

  val credentials:AWSCredentials = null
  private val client = new AmazonDynamoDBClient(credentials)

  def test {
    try {
      val forumName = "Amazon DynamoDB"
      val threadSubject = "DynamoDB Thread 1"
      // Get an item.
      getBook("101", "ProductCatalog")
      
      // Query replies posted in the past 15 days for a forum thread.
      findReplies("Reply", forumName, threadSubject)
    } catch {
      case e:Exception => println(e.getMessage())
    }  
  }
  
  def getBook(id:String, tableName:String) {
      var key = new HashMap[String, AttributeValue]()
      key.put("Id", new AttributeValue().withN(id))
      
      var getItemRequest = new GetItemRequest()
          .withTableName(tableName)
          .withKey(key)
          .withAttributesToGet(Arrays.asList("Id", "ISBN", "Title", "Authors"))
      
      var result = client.getItem(getItemRequest)

      // Check the response.
      println("Printing item after retrieving it....")
      printItem(result.getItem())            
  }

  def findReplies(tableName:String, forumName:String, threadSubject:String) {

    var replyId = forumName + "#" + threadSubject
    var twoWeeksAgoMilli = (new Date()).getTime() - (15L*24L*60L*60L*1000L)
    var twoWeeksAgo = new Date()
    twoWeeksAgo.setTime(twoWeeksAgoMilli)
    var df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    var twoWeeksAgoStr = df.format(twoWeeksAgo)
    
    var lastEvaluatedKey:JavaMap[String, AttributeValue] = null
    while (lastEvaluatedKey != null) {
      
      var hashKeyCondition = new Condition()
          .withComparisonOperator(ComparisonOperator.EQ.toString())
          .withAttributeValueList(new AttributeValue().withS(replyId))
      
      var rangeKeyCondition = new Condition()
          .withComparisonOperator(ComparisonOperator.GT.toString())
          .withAttributeValueList(new AttributeValue().withS(twoWeeksAgoStr))
      
      var keyConditions = new HashMap[String, Condition]()
      keyConditions.put("Id", hashKeyCondition)
      keyConditions.put("ReplyDateTime", rangeKeyCondition)
      
      var queryRequest = new QueryRequest().withTableName(tableName)
          .withKeyConditions(keyConditions)
          .withAttributesToGet(Arrays.asList("Message", "ReplyDateTime", "PostedBy"))
          .withLimit(1).withExclusiveStartKey(lastEvaluatedKey)   
      
      var result = client.query(queryRequest)
      for(item <- result.getItems()) {
        item:JavaMap[String,AttributeValue] =>
        item.foreach {
          case (key, value) =>
          println(key + ": " + value) 
        }
      }
      lastEvaluatedKey = result.getLastEvaluatedKey()
    }
  }

  def printItem(attributeList:JavaMap[String, AttributeValue]) {
    for(item <- attributeList.entrySet()) {
      item:JavaMap[String,AttributeValue] =>
      item.foreach {
        case (key, value) =>
        println(key + ": " + value) 
      }
    }
  }
}