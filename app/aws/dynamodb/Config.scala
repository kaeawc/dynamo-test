package aws.dynamodb

import com.amazonaws.auth._
import com.amazonaws.services.dynamodbv2._

case class Config() {

  val credentials:AWSCredentials = null
  val client: AmazonDynamoDBClient = new AmazonDynamoDBClient(credentials)
  val watchCapacity:Boolean = false

}
