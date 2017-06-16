package controllers

import javax.inject.Inject

import core.encoding.JsValueUTF8Encoding
import core.util.{DynamicActions, Instrumented}
import dal._
import com.rmn.jsonapi.Undefined
import com.rmn.jsonapi.models.TypeAliases.{GenericErrorObject, GenericErrorSource}
import com.rmn.jsonapi.models.{ResourceIdentifier, TopLevelError}
import play.api.libs.json._
import play.api.mvc._

import scala.concurrent.ExecutionContext

class DocumentController @Inject()(dr: DocumentRepository, da: DynamicActions)(implicit ec: ExecutionContext) extends Controller with Instrumented with JsValueUTF8Encoding {
  import da.AuthenticatedAction

  type NotFoundErrorResponse = TopLevelError[Undefined, Seq[GenericErrorObject], Undefined]
  def NotFoundErrorResponse(msg: String, param: String) = new NotFoundErrorResponse(Undefined, Seq(GenericErrorObject(
    status = Some("404"),
    title = Some("Not Found"),
    detail = Some(msg),
    source = Some(GenericErrorSource(parameter = Some(param)))
  )), Undefined)

  def get(resourceType: String, resourceId: String) = AuthenticatedAction.async { request =>
    metrics.timer("get", resourceType).timeFuture {
      dr.resourceToDocument(ResourceIdentifier(resourceType, resourceId)).map {
        case None => NotFound(Json.toJson(NotFoundErrorResponse(s"No $resourceType with id '$resourceId' found", "resourceId")))
        case Some(doc) => Ok(doc)
      }
    }
  }

  def multiGet(resourceType: String, id: List[String]) = AuthenticatedAction.async { request =>
    metrics.timer("multiGet", resourceType).timeFuture {
      val identifiers = id.map(ResourceIdentifier(resourceType, _))
      dr.resourcesToDocument(identifiers).map(Ok(_))
    }
  }
}
