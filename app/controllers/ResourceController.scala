package controllers

import javax.inject.Inject

import core.encoding.JsValueUTF8Encoding
import core.util.{DynamicActions, Instrumented}
import dal._
import com.rmn.jsonapi.Undefined
import com.rmn.jsonapi.models.{Resource, ResourceIdentifier, TopLevelData, TopLevelError}
import com.rmn.jsonapi.models.TypeAliases._
import play.api.Logger
import play.api.data.validation.ValidationError
import play.api.http.LazyHttpErrorHandler
import play.api.http.Status._
import play.api.libs.json._
import play.api.mvc._
import play.api.mvc.Results._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object ResourceParser extends BodyParsers {
  val JSON_API_MEDIA_TYPE = "application/vnd.api+json"

  def jsonAPI: BodyParser[JsValue] = jsonAPI(parse.DefaultMaxTextLength)

  def jsonAPI(maxLength: Int): BodyParser[JsValue] = parse.when(
    _.contentType.exists(_.equalsIgnoreCase(JSON_API_MEDIA_TYPE)),
    parse.tolerantJson(maxLength),
    LazyHttpErrorHandler.onClientError(_, UNSUPPORTED_MEDIA_TYPE, s"Expecting $JSON_API_MEDIA_TYPE body")
  )

  // ripped from play's ContentTypes.scala, modified to use our ValidationErrorDoc
  def jsonAPI[T: Reads]: BodyParser[T] = BodyParser("json reader") { request =>
    import play.api.libs.iteratee.Execution.Implicits.trampoline
    jsonAPI(request) mapFuture {
      case Left(simpleResult) =>
        Future.successful(Left(simpleResult))
      case Right(jsValue) =>
        jsValue.validate[T] map { a =>
          Future.successful(Right(a))
        } recoverTotal { jsError =>
          Future.successful(Left(Status(BAD_REQUEST)(Json.toJson(ValidationTypes.ValidationErrorDoc(jsError.errors)))))
        }
    }
  }
}

object ValidationTypes {
  type ValidationErrorDoc = TopLevelError[Undefined, Seq[GenericErrorObject], Undefined]

  def ValidationErrorDoc(errors: Seq[(JsPath, Seq[ValidationError])]) =
    new ValidationErrorDoc(
      Undefined,
      for {
        (path, pathErrors) <- errors
        error <- pathErrors
        message <- error.messages
      } yield GenericErrorObject(detail = Some(message), source = Some(GenericErrorSource(pointer = Some(path.toString)))),
      Undefined
    )

  def SequenceErrorDoc(errors: Seq[(JsPath, ResourceIdentifier)]) = {
    val errorObjects = errors.map { e =>
      GenericErrorObject(
        status = Some("409"),
        title = Some("Conflict"),
        source = Some(GenericErrorSource(pointer = Some(e._1.toString))),
        detail = Some(Json.toJson(e._2).toString)
      )
    }
    new ValidationErrorDoc(Undefined, errorObjects, Undefined)
  }

  type ErrorResponse = TopLevelError[Undefined, Seq[GenericErrorObject], Undefined]
  def ServerErrorResponse(msg: String) = new ErrorResponse(Undefined, Seq(GenericErrorObject(
    status = Some("500"),
    title = Some("Internal Server Error"),
    detail = Some(msg)
  )), Undefined)

  def ClientErrorResponse(status: Int, message: String, title: String = "Client Error") = new ErrorResponse(Undefined, Seq(GenericErrorObject(
    status = Some(status.toString),
    title = Some(title),
    detail = Some(message)
  )), Undefined)
}

object ResourceController {
  import ValidationTypes._

  case class SequencedMeta(sequenceNumber: Option[Long])
  object SequencedMeta {
    implicit val formats = Json.format[SequencedMeta]
  }

  type SequencedResource = Resource[GenericAttributeType, GenericRelationshipType, GenericResourceLinksType, Option[SequencedMeta]]
  def SequencedResource(`type`: String, id: String, attributes: GenericAttributeType = None, relationships: GenericRelationshipType = None, links: GenericResourceLinksType = None, meta: Option[SequencedMeta] = None) = new SequencedResource(`type`, id, attributes, relationships, links, meta)

  type SingleResourceDoc = TopLevelData[Undefined, SequencedResource, Undefined, Undefined, Option[Seq[SequencedResource]]]
  def SingleResourceDoc(resource: SequencedResource, included: Option[Seq[SequencedResource]]) = new SingleResourceDoc(Undefined, resource, Undefined, Undefined, included)

  type MultiResourceDoc = TopLevelData[Undefined, Seq[SequencedResource], Undefined, Undefined, Option[Seq[SequencedResource]]]
  def MultiResourceDoc(resources: Seq[SequencedResource], included: Option[Seq[SequencedResource]]) = new MultiResourceDoc(Undefined, resources, Undefined, Undefined, included)

}

class ResourceController @Inject()(rr: ResourceRepository, da: DynamicActions)(implicit ec: ExecutionContext) extends Controller with Instrumented with JsValueUTF8Encoding {
  import da.{AuthenticatedAction, WriteAuthAction}
  import ResourceController._

  def contentSequenceHeader(request: Request[Any]): Option[Long] = request.headers.get("Content-Sequence").map(_.toLong)

  def contentSequenceMeta(resource: GenericResource): Option[Long] = resource.meta.flatMap(js => (js \ "contentSequence").asOpt[Long])

  val getIncomingTimer = timing("getIncoming")

  def getIncoming(resourceType: String, resourceId: String, incomingType: String) = AuthenticatedAction.async { request =>
    getIncomingTimer(for {
      maybeResource <- rr.get(resourceType, resourceId)
      incomingResources <- rr.getIncoming(resourceType, resourceId, incomingType)
    } yield {
      maybeResource.map(r => Ok(Json.toJson(SingleResourceDoc(r, Some(incomingResources))))).getOrElse(NotFound)
    })
  }

  def put(resourceType: String, resourceId: String) = WriteAuthAction.async(ResourceParser.jsonAPI[SingleResourceDoc]) { request =>
    metrics.timer("put", resourceType).timeFuture {
      val upload = contentSequenceHeader(request) match {
        case None => request.body.data
        case Some(seqNum) =>
          val newMeta: Option[SequencedMeta] = Some(SequencedMeta(Some(seqNum)))
          request.body.data.copy(meta = newMeta)
      }
      if (upload.`type` != resourceType || upload.id != resourceId) {
        Future.successful(UnprocessableEntity)
      } else {
        rr.save(upload).map {
          case ResourceRepository.Created => Created
          case ResourceRepository.Updated => Ok
          case ResourceRepository.NotModified => NoContent
          case ResourceRepository.OutOfSequence => Conflict
        } recover {
          case NonFatal(e) =>
            Logger.error("ResourceRepository::saveResource failed", e)
            InternalServerError
        }
      }
    }
  }

  def multiPut() = WriteAuthAction.async(ResourceParser.jsonAPI[MultiResourceDoc]) { request =>
    val identifiers = request.body.data.map(r => ResourceIdentifier(r.`type`, r.id))
    if (identifiers.length > 10000)
      Future.successful(EntityTooLarge)
    else if (identifiers.length != identifiers.toSet.size)
      Future.successful(UnprocessableEntity)
    else rr.saveAll(request.body.data).map { results =>
      val outOfSequence = results.filter(_._2 == ResourceRepository.OutOfSequence).keySet
      if (outOfSequence.isEmpty)
        Ok
      else
        Conflict(Json.toJson(ValidationTypes.SequenceErrorDoc(for {
          (identifier, i) <- identifiers.zipWithIndex
          if outOfSequence.contains(identifier)
        } yield {
          (JsPath \ "data" \ i, identifier)
        })))
    }
  }

  def delete(resourceType: String, resourceId: String) = WriteAuthAction.async { request =>
    metrics.timer("delete", resourceType).timeFuture {
      rr.delete(resourceType, resourceId, contentSequenceHeader(request)).map {
        case ResourceRepository.Created | ResourceRepository.Updated => Ok
        case ResourceRepository.NotModified => NoContent
        case ResourceRepository.OutOfSequence => Conflict
      } recover {
        case NonFatal(e) =>
          Logger.error("ResourceRepository::deleteResource failed", e)
          InternalServerError
      }
    }
  }
}
