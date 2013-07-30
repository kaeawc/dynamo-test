package aws.dynamodb

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ComparisonOperator, Condition}
import java.util.{HashMap,Map => JavaMap}
import scala.collection.JavaConversions._

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

  import ComparisonOperator._

  def greaterThan(name:String,value:Any,state:Option[Where] = None) = is(name,GT, value, state)

  def lessThan(name:String,value:Any,state:Option[Where] = None) = is(name,LT, value, state)

  def equalTo(name:String,value:Any,state:Option[Where] = None) = is(name,EQ, value, state)

  def lessThanOrEqualTo(name:String,value:Any,state:Option[Where] = None) = is(name,ComparisonOperator.LE, value, state)

  def greaterThanOrEqualTo(name:String,value:Any,state:Option[Where] = None) = is(name,ComparisonOperator.GE, value, state)

  def notEqualTo(name:String,value:Any,state:Option[Where] = None) = is(name,ComparisonOperator.NE, value, state)

  def doesNotContain(name:String,value:Any,state:Option[Where] = None) = is(name,ComparisonOperator.NOT_CONTAINS, value, state)

  def contains(name:String,value:Any,state:Option[Where] = None) = is(name,ComparisonOperator.CONTAINS, value, state)

  def notNull(name:String,value:Any,state:Option[Where] = None) = is(name,ComparisonOperator.NOT_NULL, value, state)

  def isNull(name:String,value:Any,state:Option[Where] = None) = is(name,ComparisonOperator.NULL, value, state)

  def beginsWith(name:String,value:Any,state:Option[Where] = None) = is(name,ComparisonOperator.BEGINS_WITH, value, state)

  def in(name:String,values:List[Any],state:Option[Where] = None) = {

    val list = values.foldLeft(List[AttributeValue]()) {
      (a,b) => a ++ List[AttributeValue](Value.from(b))
    }

    enforce(
      name,
      new Condition()
        .withComparisonOperator(BETWEEN)
        .withAttributeValueList(list),
      state)
  }

  def between(name:String,start:Any,end:Any,state:Option[Where] = None):Where =
    enforce(
      name,
      new Condition()
      .withComparisonOperator(BETWEEN)
      .withAttributeValueList(Value.from(start), Value.from(end)),
      state)

  private def enforce(name:String,newCondition:Condition,state:Option[Where]):Where =
    if(state.isEmpty)
      Where().and(name,newCondition)
    else
      state.get.and(name,newCondition)

  def is(name:String,operator:ComparisonOperator,value:Any,state:Option[Where] = None):Where =
    enforce(
      name,
      new Condition()
      .withComparisonOperator(operator)
      .withAttributeValueList(Value.from(value)),
      state)

}