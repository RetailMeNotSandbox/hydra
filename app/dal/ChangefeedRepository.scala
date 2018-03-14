package dal

import javax.inject.{Inject, Singleton}

import com.github.tminglei.slickpg.utils.PlainSQLUtils
import com.google.inject.ImplementedBy
import core.db.{DatabaseModule, PgDriver}
import core.util.{FutureUtil, Instrumented}
import org.joda.time.DateTime
import slick.dbio.Effect.{Read, Write}
import slick.jdbc.GetResult.GetLong
import slick.jdbc.{GetResult, GetTupleResult}

import scala.concurrent.{ExecutionContext, Future}

@ImplementedBy(classOf[PostgresChangefeedRepository])
trait ChangefeedRepository {
  def list(page: Int, size: Int): Future[Seq[(ChangefeedRow, Long, Option[DateTime])]]
  def get(id: String) : Future[Option[(ChangefeedRow, Long, Option[DateTime])]]
  def simpleGet(id: String): Future[Option[ChangefeedRow]]
  def create(id: String, typeFilter: Option[List[String]], parentId: Option[String]): Future[Int]
  def delete(id: String) : Future[Int]
  def ack(id: String, ack: Long) : Future[Int]
}

@Singleton
class PostgresChangefeedRepository @Inject()(databaseModule: DatabaseModule)(implicit ec: ExecutionContext) extends ChangefeedRepository with Instrumented {
  import PgDriver.api._
  import Tables._

  val listTimer = timing("list")
  def list(page: Int, size: Int): Future[Seq[(ChangefeedRow, Long, Option[DateTime])]] = listTimer {
    val action: DBIOAction[Seq[(ChangefeedRow, Long, Option[DateTime])], NoStream, Read] = sql"""
      SELECT
        c.*,
        COALESCE(parent.max_ack, (SELECT MAX(seq) FROM change_history), 0),
        (SELECT h.event_time FROM change_history h WHERE h.type = ANY(c.type_filter) ORDER BY h.seq DESC LIMIT 1)
      FROM
        changefeed AS c
        LEFT JOIN changefeed AS parent ON parent.id = c.parent_id
      ORDER BY c.id
      OFFSET $page * $size
      LIMIT $size
    """.as[(ChangefeedRow, Long, Option[DateTime])]
    databaseModule.read.run(action)
  }.andThen(FutureUtil.logFailure("ChangefeedRepo.list"))

  val getTimer = timing("get")
  def get(id: String): Future[Option[(ChangefeedRow, Long, Option[DateTime])]] = getTimer {
    val action: DBIOAction[Option[(ChangefeedRow, Long, Option[DateTime])], NoStream, Read] = sql"""
      SELECT
        c.*,
        COALESCE(parent.max_ack, (SELECT MAX(seq) FROM change_history), 0),
        (SELECT h.event_time FROM change_history h WHERE h.type = ANY(c.type_filter) ORDER BY h.seq DESC LIMIT 1)
      FROM
        changefeed AS c
        LEFT JOIN changefeed AS parent ON parent.id = c.parent_id
        WHERE c.id = $id;
    """.as[(ChangefeedRow, Long, Option[DateTime])].headOption
    databaseModule.read.run(action)
  }.andThen(FutureUtil.logFailure("ChangefeedRepo.get"))

  val simpleGetTimer = timing("simpleGet")
  def simpleGet(id: String): Future[Option[ChangefeedRow]] = simpleGetTimer(databaseModule.read.run{
    Tables.Changefeed
      .filter(_.id === id)
      .result
      .headOption
  }).andThen(FutureUtil.logFailure("ChangefeedRepo.simpleGet"))

  val insertTarget = Tables.Changefeed
    .map(r => (r.id, r.typeFilter, r.parentId))

  val createTimer = timing("create")
  def create(id: String, typeFilter: Option[List[String]], parentId: Option[String]): Future[Int] = createTimer(databaseModule.write.run {
    insertTarget += ((id, typeFilter, parentId))
  }).andThen(FutureUtil.logFailure("ChangefeedRepo.create"))

  val deleteTimer = timing("delete")
  def delete(id: String) : Future[Int] = deleteTimer(databaseModule.write.run{
    Tables.Changefeed
      .filter(_.id === id)
      .delete
  }).andThen(FutureUtil.logFailure("ChangefeedRepo.delete"))

  val ackTimer = timing("ack")
  def ack(id: String, ack: Long) : Future[Int] = {
    val action: DBIOAction[Int, NoStream, Write] = sqlu"""
      UPDATE changefeed
      SET
        max_ack = GREATEST(max_ack, $ack),
        last_ack = CASE WHEN max_ack < $ack THEN CURRENT_TIMESTAMP ELSE last_ack END
      WHERE id = $id
    """
    ackTimer(databaseModule.write.run(action)).andThen(FutureUtil.logFailure("ChangefeedRepo.ack"))
  }
}
