package aws.dynamodb

import com.amazonaws.services.dynamodbv2.model.{ComparisonOperator, Condition}
import java.util.{HashMap,Map => JavaMap}

case class Where(conditions:JavaMap[String, Condition] = new HashMap[String,Condition]()) {

  def and(name:String,newCondition:Condition):Where = {
    conditions.put(name,newCondition)
    this
  }

  def greaterThan(name:String,value:_) =
    Where.greaterThan(name, value, Some(this))

  def lessThan(name:String,value:_) =
    Where.lessThan(name, value, Some(this))

  def equalTo(name:String,value:_) =
    Where.equalTo(name, value, Some(this))

  def is(name:String,operator:ComparisonOperator,value:_):Where =
    Where.is(name,operator, value, Some(this))

}

object Where {


  def greaterThan(name:String,value:_,state:Option[Where] = None) = is(name,ComparisonOperator.GT, value, state)

  def lessThan(name:String,value:_,state:Option[Where] = None) = is(name,ComparisonOperator.LT, value, state)

  def equalTo(name:String,value:_,state:Option[Where] = None) = is(name,ComparisonOperator.EQ, value, state)

  def is(name:String,operator:ComparisonOperator,value:_,state:Option[Where] = None):Where = {

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