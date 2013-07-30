package aws.dynamodb

import com.amazonaws.services.dynamodbv2.model.{ComparisonOperator, Condition}
import java.util.{HashMap,Map => JavaMap}

case class Where(conditions:JavaMap[String, Condition] = new HashMap[String,Condition]()) {

  def and(name:String,newCondition:Condition):Where = {
    conditions.put(name,newCondition)
    this
  }

  def greaterThan(name:String,value:Any) =
    Where.greaterThan(name, value, Some(this))

  def lessThan(name:String,value:Any) =
    Where.lessThan(name, value, Some(this))

  def equalTo(name:String,value:Any) =
    Where.equalTo(name, value, Some(this))

  def is(name:String,operator:ComparisonOperator,value:Any):Where =
    Where.is(name,operator, value, Some(this))

}

object Where {


  def greaterThan(name:String,value:Any,state:Option[Where] = None) = is(name,ComparisonOperator.GT, value, state)

  def lessThan(name:String,value:Any,state:Option[Where] = None) = is(name,ComparisonOperator.LT, value, state)

  def equalTo(name:String,value:Any,state:Option[Where] = None) = is(name,ComparisonOperator.EQ, value, state)

  def is(name:String,operator:ComparisonOperator,value:Any,state:Option[Where] = None):Where = {

    val newCondition = new Condition()
      .withComparisonOperator(operator.toString)
      .withAttributeValueList(Value.from(value))

    if(state.isEmpty) {
      Where().and(name,newCondition)
    } else {
      state.get.and(name,newCondition)
    }
  }

}