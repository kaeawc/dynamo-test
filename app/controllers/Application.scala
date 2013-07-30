package controllers

import play.api._
import play.api.mvc._
import play.api.libs.json._
import models.Widget
object Application extends Controller {
  
  def index = Action {
    Ok("")
  }
  
  def get(id:Long) = Action {
    Widget.getById(id).map {
    	result =>
    	Ok(Json.toJson(result))
    }.getOrElse {
    	InternalServerError(Json.obj("reason" -> "DynamoDB Request Died."))
    }
  }
  
  def post(name:String) = Action {
    Widget.create(name).map {
    	result =>
    	Ok(Json.toJson(result))
    }.getOrElse {
    	InternalServerError(Json.obj("reason" -> "DynamoDB Request Died."))
    }
  }
  
  def put(id:Long,name:String) = Action {
    Widget.update(id,name).map {
    	result =>
    	Ok(Json.toJson(result))
    }.getOrElse {
    	InternalServerError(Json.obj("reason" -> "DynamoDB Request Died."))
    }
  }
  
  def delete(id:Long) = Action {
    Widget.delete(id).map {
    	result =>
    	Ok(Json.toJson(result))
    }.getOrElse {
    	InternalServerError(Json.obj("reason" -> "DynamoDB Request Died."))
    }
  }
  
}