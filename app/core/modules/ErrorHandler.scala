package core.modules

import play.api.http.HttpErrorHandler
import play.api.mvc._
import play.api.mvc.Results._
import play.api.libs.json._

import scala.concurrent._

class ErrorHandler extends HttpErrorHandler {
  import controllers.ValidationTypes._

  def onClientError(request: RequestHeader, statusCode: Int, message: String) = {
    Future.successful(
      Status(statusCode)(Json.toJson(ClientErrorResponse(statusCode, message)))
    )
  }

  def onServerError(request: RequestHeader, exception: Throwable) = {
//    Logger.error(s"Error during request to ${request.path}", exception)
    Future.successful(
      InternalServerError(Json.toJson(ServerErrorResponse(s"Error: ${exception.getMessage}  Caused by: ${exception.getCause}")))
    )
  }
}
