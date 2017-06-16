package dal

import javax.inject.{Inject, Singleton}

import com.google.inject.ImplementedBy
import core.db.{DatabaseModule, PgDriver}
import core.util.{FutureUtil, Instrumented}
import com.rmn.jsonapi.models.ResourceIdentifier
import play.api.db.slick.DatabaseConfigProvider
import play.api.libs.json._
import slick.dbio.Effect.Read

import scala.concurrent.{ExecutionContext, Future}

@ImplementedBy(classOf[PostgresDocumentRepository])
trait DocumentRepository {
  def resourceToDocument(id: ResourceIdentifier) : Future[Option[JsValue]]
  def resourcesToDocument(ids: Seq[ResourceIdentifier]) : Future[JsValue]
}

@Singleton
class PostgresDocumentRepository @Inject()(databaseModule: DatabaseModule)(implicit ec: ExecutionContext) extends DocumentRepository with Instrumented {
  import PgDriver.api._

  // regarding the recursive queries below
  // -------------------------------------
  // apparently it's cheaper to not hit the resource_references table (done when there was an index on (from_id, to_id))
  //
  // querying against resource_references:
  //
  //  explain WITH RECURSIVE outgoing(id) AS (
  //      SELECT rr.to_id
  //      FROM resource_references rr
  //      WHERE rr.from_id = '{"id": "<uuid>", "type": "<type>"}'
  //    UNION
  //      SELECT rr.to_id
  //      FROM resource_references rr
  //      INNER JOIN outgoing ON rr.from_id = outgoing.id
  //    )
  //    SELECT jsonb_build_object(
  //      'data', resource.resource,
  //      'included', COALESCE((SELECT jsonb_agg(r.resource) FROM resource r WHERE r.id = ANY(ARRAY(select o.id from outgoing o)) AND NOT r.deleted), '[]'::jsonb)
  //    )
  //    FROM resource
  //    WHERE resource.id = '{"id": "<uuid>", "type": "<type>"}'
  //      AND NOT resource.deleted;
  //
  //                                                                QUERY PLAN
  //  --------------------------------------------------------------------------------------------------------------------------------------
  //   Index Scan using resource_pkey on resource  (cost=9229735.92..9229737.94 rows=1 width=318)
  //     Index Cond: (id = '{"id": "<uuid>", "type": "<type>"}'::jsonb)
  //     Filter: (NOT deleted)
  //     CTE outgoing
  //       ->  Recursive Union  (cost=0.69..8241677.74 rows=49401589 width=63)
  //             ->  Index Only Scan using resource_references_pkey on resource_references rr  (cost=0.69..3076.02 rows=6139 width=63)
  //                   Index Cond: (from_id = '{"id": "<uuid>", "type": "<type>"}'::jsonb)
  //             ->  Nested Loop  (cost=0.69..725056.99 rows=4939545 width=63)
  //                   ->  WorkTable Scan on outgoing  (cost=0.00..1227.80 rows=61390 width=32)
  //                   ->  Index Only Scan using resource_references_pkey on resource_references rr_1  (cost=0.69..10.99 rows=80 width=124)
  //                         Index Cond: (from_id = outgoing.id)
  //     InitPlan 3 (returns $4)
  //       ->  Aggregate  (cost=988057.60..988057.61 rows=1 width=318)
  //             InitPlan 2 (returns $3)
  //               ->  CTE Scan on outgoing o  (cost=0.00..988031.78 rows=49401589 width=32)
  //             ->  Index Scan using resource_pkey on resource r  (cost=0.56..25.79 rows=10 width=318)
  //                   Index Cond: (id = ANY ($3))

  // querying against resource.out_refs
  //
  //        explain WITH RECURSIVE outgoing(out_refs, resource) AS (
  //          SELECT resource.out_refs, resource.resource
  //          FROM resource
  //            INNER JOIN resource AS root_resource ON resource.id = ANY(root_resource.out_refs)
  //          WHERE root_resource.id = '{"id": "<uuid>", "type": "<type>"}'
  //            AND NOT resource.deleted
  //        UNION
  //          SELECT resource.out_refs, resource.resource
  //          FROM resource
  //            INNER JOIN outgoing ON resource.id = ANY(outgoing.out_refs)
  //          WHERE NOT resource.deleted
  //      )
  //      SELECT jsonb_build_object(
  //        'data', resource.resource,
  //        'included', COALESCE((SELECT jsonb_agg(o.resource) FROM outgoing AS o), '[]'::jsonb)
  //      )
  //      FROM resource
  //      WHERE resource.id = '{"id": "<uuid>", "type": "<type>"}'
  //        AND NOT resource.deleted;
  //
  //                                                          QUERY PLAN
  //  --------------------------------------------------------------------------------------------------------------------------
  //   Index Scan using resource_pkey on resource  (cost=26354.69..26356.71 rows=1 width=319)
  //     Index Cond: (id = '{"id": "<uuid>", "type": "<type>"}'::jsonb)
  //     Filter: (NOT deleted)
  //     CTE outgoing
  //       ->  Recursive Union  (cost=1.12..26129.57 rows=9980 width=351)
  //             ->  Nested Loop  (cost=1.12..28.47 rows=10 width=351)
  //                   ->  Index Scan using resource_pkey on resource root_resource  (cost=0.56..2.58 rows=1 width=32)
  //                         Index Cond: (id = '{"id": "<uuid>", "type": "<type>"}'::jsonb)
  //                   ->  Index Scan using resource_pkey on resource resource_1  (cost=0.56..25.79 rows=10 width=412)
  //                         Index Cond: (id = ANY (root_resource.out_refs))
  //                         Filter: (NOT deleted)
  //             ->  Nested Loop  (cost=0.56..2590.15 rows=997 width=351)
  //                   ->  WorkTable Scan on outgoing  (cost=0.00..2.00 rows=100 width=32)
  //                   ->  Index Scan using resource_pkey on resource resource_2  (cost=0.56..25.78 rows=10 width=412)
  //                         Index Cond: (id = ANY (outgoing.out_refs))
  //                         Filter: (NOT deleted)
  //     InitPlan 2 (returns $4)
  //       ->  Aggregate  (cost=224.55..224.56 rows=1 width=32)
  //             ->  CTE Scan on outgoing o  (cost=0.00..199.60 rows=9980 width=32)
  //  (19 rows)

  val resourceToDocumentTimer = timing("resourceToDocument")
  def resourceToDocument(identifier: ResourceIdentifier) : Future[Option[JsValue]] = resourceToDocumentTimer {
    val hydrateCommand: DBIOAction[Option[JsValue], NoStream, Read] = sql"""
      WITH RECURSIVE outgoing(out_refs, resource) AS (
          SELECT resource.out_refs, resource.resource
          FROM resource
            INNER JOIN resource AS root_resource ON resource.id = ANY(root_resource.out_refs)
          WHERE root_resource.id = ${Json.toJson(identifier)}
            AND NOT resource.deleted
        UNION
          SELECT resource.out_refs, resource.resource
          FROM resource
            INNER JOIN outgoing ON resource.id = ANY(outgoing.out_refs)
          WHERE NOT resource.deleted
      )
      SELECT jsonb_build_object(
        'data', resource.resource,
        'included', COALESCE((SELECT jsonb_agg(o.resource) FROM outgoing AS o), '[]'::jsonb)
      )
      FROM resource
      WHERE resource.id = ${Json.toJson(identifier)}
        AND NOT resource.deleted
    """.as[JsValue].headOption
    databaseModule.read.run(hydrateCommand).andThen(FutureUtil.logFailure("DocumentRepo.singleResource"))
  }

  val resourcesToDocumentTimer = timing("resourcesToDocument")
  def resourcesToDocument(ids: Seq[ResourceIdentifier]) : Future[JsValue] = resourcesToDocumentTimer {
    val idList = ids.toList.map(Json.toJson(_))
    val hydrateCommand: DBIOAction[JsValue, NoStream, Read] = sql"""
      WITH RECURSIVE outgoing(out_refs, resource) AS (
          SELECT resource.out_refs, resource.resource
          FROM resource
            INNER JOIN resource AS root_resource ON resource.id = ANY(root_resource.out_refs)
          WHERE root_resource.id = ANY($idList)
            AND NOT resource.deleted
        UNION
          SELECT resource.out_refs, resource.resource
          FROM resource
            INNER JOIN outgoing ON resource.id = ANY(outgoing.out_refs)
          WHERE NOT resource.deleted
      )
      SELECT jsonb_build_object(
        'data', COALESCE(jsonb_agg(resource.resource), '[]'::jsonb),
        'included', COALESCE((SELECT jsonb_agg(o.resource) FROM outgoing AS o), '[]'::jsonb)
      )
      FROM resource
      WHERE resource.id = ANY(ARRAY(SELECT jsonb_array_elements(${Json.toJson(ids)})))
        AND NOT resource.deleted
    """.as[JsValue].head
    databaseModule.read.run(hydrateCommand).andThen(FutureUtil.logFailure("DocumentRepo.manyResource"))
  }
}
