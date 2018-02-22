package controllers

import java.util.concurrent.TimeUnit
import javax.inject.{Inject, Named}

import actors.{ChangefeedFetcher, ChangefeedSource}
import actors.ChangefeedSource.ChangefeedEvent
import akka.actor.ActorRef
import akka.stream._
import akka.stream.scaladsl.Source
import com.google.inject.ImplementedBy
import core.encoding.JsValueUTF8Encoding
import core.stream.Ops
import core.util.{DynamicActions, Instrumented}
import dal._
import com.rmn.jsonapi.{ReadRefine, Undefined}
import com.rmn.jsonapi.models._
import org.joda.time.{DateTime, DateTimeZone}
import play.Configuration
import play.api.Logger
import play.api.http.Writeable
import play.api.libs.json._
import play.api.mvc._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

case class ChangefeedAttributes(maxAck: Long, parentMaxAck: Long, maxEventTime: Option[DateTime], typeFilter: Option[List[String]], created: DateTime, lastAck: DateTime)
object ChangefeedAttributes {
  implicit val jodaWrites : Writes[DateTime] = Writes[DateTime](ldt => {
    JsString(ldt.withZone(DateTimeZone.UTC).toString())
  })
  implicit val format = Json.format[ChangefeedAttributes]
}

class ChangefeedResourceId(id: String) extends ResourceIdentifier("changefeed", id)
object ChangefeedResourceId {
  implicit val fmt = ReadRefine { ri: ResourceIdentifier =>
    if (ri.`type` != "changefeed") {
      JsError(s"Expected type 'changefeed', found '${ri.`type`}'.")
    } else {
      JsSuccess(new ChangefeedResourceId(ri.id))
    }
  }
}

case class ChangefeedRelationships(parent: Option[RelationshipObject[Undefined, ChangefeedResourceId, Undefined]])
object ChangefeedRelationships {
  implicit val format : OFormat[ChangefeedRelationships] = Json.format[ChangefeedRelationships]
}

class ChangefeedResource(id: String, attributes: ChangefeedAttributes, relationships: Option[ChangefeedRelationships]) extends Resource[ChangefeedAttributes, Option[ChangefeedRelationships], Undefined, Undefined]("changefeed", id, attributes, relationships, Undefined, Undefined)
object ChangefeedResource {
  implicit val format : Format[ChangefeedResource] = ReadRefine { in : Resource[ChangefeedAttributes, Option[ChangefeedRelationships], Undefined, Undefined] =>
    if (in.`type` != "changefeed") {
      JsError(s"Expected type 'changefeed', found '${in.`type`}'.")
    } else {
      JsSuccess(new ChangefeedResource(in.id, in.attributes, in.relationships))
    }
  }
}

case class ChangefeedCreateAttributes(typeFilter: Option[List[String]])
object ChangefeedCreateAttributes {
  implicit val format = Json.format[ChangefeedCreateAttributes]
}
class ChangefeedCreateResource(id: String, attributes: ChangefeedCreateAttributes, relationships: Option[ChangefeedRelationships])
  extends Resource[ChangefeedCreateAttributes, Option[ChangefeedRelationships], Undefined, Undefined]("changefeed", id, attributes, relationships, Undefined, Undefined)
object ChangefeedCreateResource {
  implicit val format : Format[ChangefeedCreateResource] = ReadRefine { in : Resource[ChangefeedCreateAttributes, Option[ChangefeedRelationships], Undefined, Undefined] =>
    if (in.`type` != "changefeed") {
      JsError(s"Expected type 'changefeed', found '${in.`type`}'.")
    } else {
      JsSuccess(new ChangefeedCreateResource(in.id, in.attributes, in.relationships))
    }
  }
}

@ImplementedBy(classOf[DIChangeSourceBuilder])
trait ChangeSourceBuilder {
  def buildSource(changefeed: ChangefeedRow, maxUnackedCount: Int, delay: FiniteDuration) : Source[ChangefeedEvent, _]
}

class DIChangeSourceBuilder @Inject() (cr: ChangefeedRepository, history: ChangeHistoryRepository, @Named("subscription-manager") subMgr: ActorRef, config: Configuration) extends ChangeSourceBuilder {
  def buildSource(changefeed: ChangefeedRow, maxUnackedCount: Int, delay: FiniteDuration) : Source[ChangefeedEvent, _] = {
    val timeout = FiniteDuration(config.getMilliseconds("hydra.changefeed.timeout"), TimeUnit.MILLISECONDS)
    val publisher = ChangefeedSource.props(timeout, _.actorOf(ChangefeedFetcher.props(changefeed, subMgr, history, cr, maxUnackedCount, timeout)))

    val source = Source.actorPublisher[ChangefeedEvent](publisher)
    val withWindow = if (timeout.length > 0) {
      source.via(Ops.dedupeWindow[ChangefeedEvent, (String, String)](delay, maxUnackedCount, DelayOverflowStrategy.backpressure, {
        case ChangefeedEvent("event", Some(ChangeHistoryRow(itemType, id, _, _)), _) => itemType -> id
      }, _.eventType == "error"))
    } else {
      source
    }

    withWindow.keepAlive(timeout, () => ChangefeedEvent("keepalive"))
  }
}

class ChangefeedController @Inject()(cr: ChangefeedRepository, da: DynamicActions, builder: ChangeSourceBuilder)(implicit ec: ExecutionContext) extends Controller with Instrumented with JsValueUTF8Encoding {
  import ValidationTypes._
  import da.{AuthenticatedAction, WriteAuthAction}

  type ChangefeedListLinks = FullLinks[String, Undefined, String, Undefined, Option[String], Option[String]]
  type ChangefeedListResponse = TopLevelData[Undefined, Seq[ChangefeedResource], Undefined, ChangefeedListLinks, Undefined]

  def constrain(min: Int, value: Int) = Math.max(min, value)
  def constrain(min: Int, value: Int, max: Int) = Math.min(Math.max(min, value), max)

  def changefeedToResource(t: (ChangefeedRow, Long, Option[DateTime])) : ChangefeedResource = new ChangefeedResource(
    t._1.id,
    ChangefeedAttributes(
      maxAck = t._1.maxAck,
      parentMaxAck = t._2,
      maxEventTime = t._3,
      typeFilter = t._1.typeFilter,
      created = t._1.created,
      lastAck = t._1.lastAck
    ),
    Some(ChangefeedRelationships(
      t._1.parentId.map(parentId => RelationshipObject(Undefined, new ChangefeedResourceId(parentId), Undefined))
    ))
  )

  def list(page: Int, size: Int) = AuthenticatedAction.async {
    val safePage = constrain(0, page)
    val safeSize = constrain(1, size, 100)

    cr.list(safePage, safeSize).map { data =>
      val self = routes.ChangefeedController.list(safePage, safeSize).toString
      val firstLink = routes.ChangefeedController.list(0, safeSize).toString
      val nextLink = if (data.length == size) { Some(routes.ChangefeedController.list(page + 1, safeSize).toString) } else None
      val prevLink = if (safePage > 0) { Some(routes.ChangefeedController.list(page - 1, safeSize).toString) } else None

      Ok(Json.toJson(new ChangefeedListResponse(Undefined, data.map(changefeedToResource), Undefined, new ChangefeedListLinks(self, Undefined, firstLink, Undefined, prevLink, nextLink), Undefined)))
    }
  }

  type ChangefeedCreateDoc = TopLevelData[Undefined, ChangefeedCreateResource, Undefined, Undefined, Undefined]
  type ChangefeedGetDoc = TopLevelData[Undefined, ChangefeedResource, Undefined, Undefined, Undefined]
  def ChangefeedGetDoc(resource: ChangefeedResource) = new ChangefeedGetDoc(Undefined, resource, Undefined, Undefined, Undefined)

  def create = WriteAuthAction.async(ResourceParser.jsonAPI[ChangefeedCreateDoc]) { request =>
    val changefeed = request.body.data
    val typeFilter = changefeed.attributes.typeFilter
    val parentId = changefeed.relationships.flatMap(_.parent.map(_.data.id))
    for {
      insertCount <- cr.create(changefeed.id, typeFilter, parentId)
      lookup <- cr.get(changefeed.id)
    } yield {
      lookup match {
        case Some(results) => Ok(Json.toJson(ChangefeedGetDoc(changefeedToResource(results))))
        case None =>
          Logger.error(s"Tried to create a changefeed, but couldn't retrieve it.  Insert returned $insertCount")
          InternalServerError
      }
    }
  }
  def get(id: String) = AuthenticatedAction.async {
    cr.get(id).map {
      case Some(results) => Ok(Json.toJson(ChangefeedGetDoc(changefeedToResource(results))))
      case None => NotFound
    }
  }
  def delete(id: String) = WriteAuthAction.async {
    cr.delete(id).map {
      case 0 => NotFound
      case 1 => NoContent
    }
  }

  implicit val rowFmt = Json.format[ChangeHistoryRow]
  implicit val eventFmt = Json.format[ChangefeedEvent]
  implicit val eventWriter = new Writeable[ChangefeedEvent]({ event =>
    val stringified = Json.stringify(Json.toJson(event))
    Codec.utf_8.encode(stringified + "\n")
  }, Some("text/plain; charset=utf-8"))

  def stream(id: String, bufferSize: Int, delayMS: Int) = AuthenticatedAction.async {
    val safeBufferSize = constrain(1, bufferSize, 10000)
    val safeDelay = constrain(0, delayMS)
    cr.simpleGet(id).map {
      case None => NotFound
      case Some(row) => Ok.chunked(builder.buildSource(row, safeBufferSize, safeDelay.millis)).withHeaders("Connection" -> "close")
    }
  }

  def ack(id: String, ack: Long) = WriteAuthAction.async {
    cr.get(id).flatMap {
      case None => Future.successful(NotFound)
      case Some((_, parentAck, _)) =>
        if (ack > parentAck) {
          Future.successful(BadRequest(Json.toJson(ClientErrorResponse(BAD_REQUEST, s"Cannot ack $ack on changefeed '$id'.  Max ack of its parent is $parentAck"))))
        } else {
          cr.ack(id, ack).map {_ => NoContent}
        }
    }
  }
}
