package controllers

import play.api.mvc._
import play.api.libs.json._
import scala.concurrent.ExecutionContext.Implicits.global
import models.Widget

object Application extends Controller {

  def index = Action {
    Ok("")
  }

  def get(id:Long) = Action {
    Async {
      Widget.getById(id).map {
        result =>
          Ok(Json.toJson(result))
      }
    }
  }

  def post(name:String) = Action {
    Async {
      Widget.create(name).map {
        result =>
          Ok(Json.toJson(result))
      }
    }
  }

  def put(id:Long,name:String) = Action {
    Async {
      Widget.update(id,name).map {
        result =>
          Ok(Json.toJson(result))
      }
    }
  }

  def delete(id:Long) = Action {
    Async {
      Widget.delete(id).map {
        result =>
          Ok(Json.toJson(result))
      }
    }
  }

}