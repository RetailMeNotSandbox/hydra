package dal

import java.sql.Timestamp
import java.util.Date
import javax.inject.{Inject, Singleton}

import com.google.inject.ImplementedBy
import controllers.ResourceController.{SequencedMeta, SequencedResource}
import core.db.{DatabaseModule, PgDriver}
import core.util.{FutureUtil, Instrumented}
import dal.ResourceRepository.SaveResult
import com.rmn.jsonapi.models.ResourceIdentifier
import org.joda.time.{DateTime, DateTimeZone}
import play.api.db.slick.DatabaseConfigProvider
import play.api.libs.json._
import slick.backend.DatabasePublisher
import slick.dbio.Effect.{Read, Transactional}

import scala.concurrent.{ExecutionContext, Future}


// how useful/wasteful would it be to keep a table of all previous revisions of a resource? - very.  for the same reason, we're only sending ids via changefeed instead of the fully hydrated doc
object ResourceRepository {
  sealed trait SaveResult
  case object Created extends SaveResult
  case object Updated extends SaveResult
  case object NotModified extends SaveResult
  case object OutOfSequence extends SaveResult
}

@ImplementedBy(classOf[PostgresResourceRepository])
trait ResourceRepository {
  def save(resource: SequencedResource) : Future[SaveResult]
  def saveAll(resources: Seq[SequencedResource]): Future[Map[ResourceIdentifier, SaveResult]]
  def delete(resourceType: String, resourceId: String, sequenceNumber: Option[Long]) : Future[SaveResult]
  def get(resourceType: String, resourceId: String): Future[Option[SequencedResource]] = get(ResourceIdentifier(resourceType, resourceId))
  def get(identifier: ResourceIdentifier): Future[Option[SequencedResource]]
  def getIncoming(resourceType: String, resourceId: String, incomingType: String): Future[Seq[SequencedResource]]
}

@Singleton
class PostgresResourceRepository @Inject()(databaseModule: DatabaseModule)(implicit ec: ExecutionContext) extends ResourceRepository with Instrumented {
  import dal.ResourceRepository._
  import PgDriver.api._

  private def currentTimestamp: Timestamp =
    new Timestamp(new Date().getTime)

  // :(
  private def relationshipIds(resource: SequencedResource): Seq[ResourceIdentifier] =
    resource
      .relationships // let's grab the ResourceIdentifiers from 'relationships'
      .getOrElse(Map.empty) // the 'relationships' property may not be set.  Fake it with an empty map
      .values  // don't care what the name of the relationship is, just it's ResourceIdentifier
      .flatMap(  // one relationship may have multiple ResourceIdentifiers
        _.data  // ResourceIds are hiding under the 'data' property
          .map( // ...which may not be there
            _.fold( // 'data' can either be a single ResourceIdentifier, or multiple
              Seq(_),  // it's single, so lets wrap in Seq() for the parent flatmap
              identity // it's a seq, so lets use `identity` to just return it
            )
          )
          .getOrElse(Seq.empty) // and if 'data' wasn't there, let's pretend like it had an empty sequence
      // sorted and deduped so it's stable
      ).toSeq.distinct.sortBy(r => r.`type` -> r.id)

  private def upsertResourceRows(resources: Seq[SequencedResource], deleted: Boolean): Future[Map[ResourceIdentifier, SaveResult]] = {
    val now = currentTimestamp
    val upsert: DBIOAction[Seq[(JsValue, Timestamp, Timestamp)], Streaming[(JsValue, Timestamp, Timestamp)], Read] = {
      val resourceList = resources
      val idArray = resourceList.map(r => Json.toJson(ResourceIdentifier(r.`type`, r.id)))
      val resourceArray = resourceList.map(Json.toJson(_))
      val seqNumArray = resourceList.map(r => r.meta.flatMap(_.sequenceNumber))

      // :(
      // https://github.com/tminglei/slick-pg/issues/177#issuecomment-121079592
      val jsonWrappedOutRefsArray = resourceList.map(r => Json.toJson(relationshipIds(r)))

      sql"""
        INSERT INTO resource (id, deleted, resource, sequence_number, created, updated, out_refs)
          SELECT id_value, $deleted, resource_value, sequence_number_value, $now, $now, ARRAY(SELECT jsonb_array_elements(out_refs_value))
          FROM UNNEST($idArray, $resourceArray, $seqNumArray, $jsonWrappedOutRefsArray)
            AS values_to_insert (id_value, resource_value, sequence_number_value, out_refs_value)
        ON CONFLICT (id) DO UPDATE
          SET deleted = EXCLUDED.deleted,
            out_refs = EXCLUDED.out_refs,
            resource = EXCLUDED.resource,
            sequence_number = EXCLUDED.sequence_number,
            updated = CASE WHEN (
              resource.deleted <> EXCLUDED.deleted
              OR resource.resource <> EXCLUDED.resource
              OR resource.sequence_number IS DISTINCT FROM EXCLUDED.sequence_number
            ) THEN EXCLUDED.updated ELSE resource.updated END
          WHERE resource.sequence_number IS NULL
            OR resource.sequence_number < EXCLUDED.sequence_number
            OR (
              resource.sequence_number = EXCLUDED.sequence_number
              AND resource.deleted = EXCLUDED.deleted
              AND resource.resource = EXCLUDED.resource
            )
        RETURNING id, created, updated
      """.as[(JsValue, Timestamp, Timestamp)]
    }

    databaseModule.write.run(upsert).andThen(FutureUtil.logFailure("ResourceRepo.upsert")).map { rows =>
      // This looks unnecessarily complicated. And I suppose it is.
      // But we want our map to include OutOfSequence for the resources that were not inserted or updated.
      // These rows won't be returned by the insert statement at all.
      // And withDefaultValue won't actually add any OutOfSequence entries to the map (how could it?)
      val successfulSaveResults: Map[ResourceIdentifier, SaveResult] = (for {
        (id, created, updated) <- rows
        identifier = id.as[ResourceIdentifier]
        saveResult = if (created == now) Created
                     else if (updated == now) Updated
                     else NotModified
      } yield {
         identifier -> saveResult
      }).toMap

      (for {
        resource <- resources
        identifier = ResourceIdentifier(resource.`type`, resource.id)
        saveResult = successfulSaveResults.getOrElse(identifier, OutOfSequence)
      } yield {
        identifier -> saveResult
      }).toMap
    }
  }

  def save(resource: SequencedResource) : Future[SaveResult] = metrics.timer("save", resource.`type`).timeFuture {
    upsertResourceRows(Seq(resource), deleted=false).map(_.head._2)
  }

  val saveAllTimer = timing("saveAll")
  def saveAll(resources: Seq[SequencedResource]): Future[Map[ResourceIdentifier, SaveResult]] = saveAllTimer {
    upsertResourceRows(resources, deleted=false)
  }

  def delete(resourceType: String, resourceId: String, sequenceNumber: Option[Long]) : Future[SaveResult] = metrics.timer("delete").timeFuture {
    // important that resource.out_refs will be empty in the db when deleted is true! (resourceToDocument will walk out_refs of a deleted root resource)
    val resource = SequencedResource(resourceType, resourceId, meta = sequenceNumber.map(n => SequencedMeta(Some(n))))
    upsertResourceRows(Seq(resource), deleted=true).map(_.head._2)
  }

  def get(identifier: ResourceIdentifier): Future[Option[SequencedResource]] = metrics.timer("get", identifier.`type`).timeFuture {
    val select: DBIOAction[Option[JsValue], NoStream, Read] = sql"""
      SELECT resource
      FROM resource
      WHERE id = ${Json.toJson(identifier)} AND NOT deleted
    """.as[JsValue].headOption

    databaseModule.read.run(select).map(_.map(s => s.as[SequencedResource])).andThen(FutureUtil.logFailure("ResourceRepo.getResource"))
  }

  val getIncomingTimer = timing("getIncoming")
  def getIncoming(resourceType: String, resourceId: String, incomingType: String): Future[Seq[SequencedResource]] = getIncomingTimer {
    val select: DBIOAction[Seq[JsValue], NoStream, Read] = sql"""
      SELECT resource
      FROM resource
      INNER JOIN resource_references rr ON rr.from_id = resource.id
      WHERE
        NOT resource.deleted
        AND rr.from_id->>'type' = $incomingType
        AND rr.to_id = ${Json.toJson(Json.obj("type" -> resourceType, "id" -> resourceId))}
    """.as[JsValue]

    databaseModule.read.run(select).map(_.map(s => s.as[SequencedResource])).andThen(FutureUtil.logFailure("ResourceRepo.getIncoming"))
  }
}
