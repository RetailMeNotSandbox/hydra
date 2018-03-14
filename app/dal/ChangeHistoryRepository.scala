package dal

import javax.inject.{Inject, Singleton}

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.google.inject.ImplementedBy
import core.db.{DatabaseModule, PgDriver}
import core.util.{FutureUtil, Instrumented}
import slick.backend.DatabasePublisher
import slick.dbio.Effect.{Read, Write}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

@ImplementedBy(classOf[PostgresChangeHistoryRepository])
trait ChangeHistoryRepository {
  def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long): Future[Seq[ChangeHistoryRow]]
  def expand() : Future[Int]
}

@Singleton
class PostgresChangeHistoryRepository @Inject()(databaseModule: DatabaseModule)(implicit ec: ExecutionContext, mat: Materializer) extends ChangeHistoryRepository with Instrumented {
  import PgDriver.api._

  val getTimer = timing("get")
  def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long): Future[Seq[ChangeHistoryRow]] = getTimer(databaseModule.read.run {
    typeFilter match {
      case Some(List(singleType)) =>
        Tables.ChangeHistory
          .filter(r => r.`type` === singleType && r.seq > fromAck && r.seq <= toAck)
          .sortBy(_.seq.asc)
          .take(size)
          .result
      case Some(types) =>
        Tables.ChangeHistory
          .filter(r => r.`type` === types.bind.any && r.seq > fromAck && r.seq <= toAck)
          .sortBy(_.seq.asc)
          .take(size)
          .result
      case None =>
        Tables.ChangeHistory
          .filter(r => r.seq > fromAck && r.seq <= toAck)
          .sortBy(_.seq.asc)
          .take(size)
          .result
    }
  }).andThen(FutureUtil.logFailure("ChangeHistoryRepo.get"))

  val compactTimer = timing("compact")
  def compact(resourceType: String, resourceId: String): Future[Int] = compactTimer(databaseModule.backend.run(
    sqlu"""
      INSERT INTO change_history(type, id, event_time) VALUES (${resourceType}, ${resourceId}, CURRENT_TIMESTAMP)
      ON CONFLICT (type, id) DO UPDATE SET seq = GREATEST(change_history.seq, EXCLUDED.seq), event_time = CURRENT_TIMESTAMP
    """: DBIOAction[Int, NoStream, Write]
  ))

  val claimAction: DBIOAction[Option[(Long, String, String)], NoStream, Write] =
    sql"""SELECT key, type, id FROM change_history_to_expand ORDER BY key ASC LIMIT 1 FOR UPDATE SKIP LOCKED""".as[(Long, String, String)].headOption
  def findIncoming(resourceType: String, resourceId: String): DatabasePublisher[(String, String)] = databaseModule.backend.stream(
    (sql"""
      WITH RECURSIVE incoming(id) AS (
       VALUES (jsonb_build_object('type', ${resourceType}, 'id', ${resourceId}))
       UNION
       SELECT rr.from_id FROM resource_references rr INNER JOIN incoming ON rr.to_id = incoming.id
      )
      SELECT id->>'type', id->>'id' FROM incoming
    """.as[(String, String)]: DBIOAction[Vector[(String, String)], Streaming[(String, String)], Read])
    // http://stackoverflow.com/questions/31340507/what-is-the-right-way-to-work-with-slicks-3-0-0-streaming-results-and-postgresq
    // transactionally and fetchSize make sure we actually stream the data
    .transactionally.withStatementParameters(fetchSize = 1000)
  )

  def consume(row: (Long, String, String)): Future[Int] = {
    Source.fromPublisher(findIncoming(row._2, row._3))
      .mapAsyncUnordered(4)(t => compact(t._1, t._2))
      .runFold(0)(_ + _)
  }

  def deleteAction(key: Long): DBIOAction[Int, NoStream, Write] =
    sqlu"""DELETE FROM change_history_to_expand WHERE key = ${key}"""

  val expandTimer = timing("expand")
  val expandBatchSize = metrics.histogram("expandBatchSize")
  val claimTimer = metrics.timer("claim")
  def expand() : Future[Int] = {
    val claimStart = claimTimer.timerContext()
    expandTimer(databaseModule.backend.run((for {
      maybeClaimed <- claimAction
      expandedCount <- {
        claimStart.stop()
        maybeClaimed.map(claimed => DBIO.from(consume(claimed))).getOrElse(DBIO.successful(0))
      }
      _ <- maybeClaimed.map(c => deleteAction(c._1)).getOrElse(DBIO.successful(0))
    } yield expandedCount).transactionally)).andThen {
      case Success(size) => expandBatchSize += size
    }.andThen(FutureUtil.logFailure("ChangeHistoryRepo.expand"))
  }
}
