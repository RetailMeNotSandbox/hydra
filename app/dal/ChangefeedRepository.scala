package dal

import javax.inject.{Inject, Singleton}

import com.google.inject.ImplementedBy
import core.db.{DatabaseModule, PgDriver}
import core.util.{FutureUtil, Instrumented}
import slick.dbio.Effect.Write

import scala.concurrent.{ExecutionContext, Future}

@ImplementedBy(classOf[PostgresChangefeedRepository])
trait ChangefeedRepository {
  def list(page: Int, size: Int): Future[Seq[(ChangefeedRow, Long)]]
  def get(id: String) : Future[Option[(ChangefeedRow, Long)]]
  def simpleGet(id: String): Future[Option[ChangefeedRow]]
  def create(id: String, typeFilter: Option[List[String]], parentId: Option[String]): Future[Int]
  def delete(id: String) : Future[Int]
  def ack(id: String, ack: Long) : Future[Int]
}

@Singleton
class PostgresChangefeedRepository @Inject()(databaseModule: DatabaseModule)(implicit ec: ExecutionContext) extends ChangefeedRepository with Instrumented {
  import PgDriver.api._

  val listTimer = timing("list")

  def list(page: Int, size: Int): Future[Seq[(ChangefeedRow, Long)]] = listTimer(databaseModule.read.run {
    Tables.Changefeed
      .sortBy(_.id)
      .drop(page * size)
      .take(size)
      .joinLeft(Tables.Changefeed)
      .on(_.parentId === _.id)
      .map(r => (r._1, r._2.map(_.maxAck).ifNull(Tables.ChangeHistory.map(_.seq).max).ifNull(0L)))
      .result
  }).andThen(FutureUtil.logFailure("ChangefeedRepo.list"))

  val getTimer = timing("get")
  def get(id: String): Future[Option[(ChangefeedRow, Long)]] = getTimer(databaseModule.read.run {
    Tables.Changefeed
      .filter(_.id === id)
      .joinLeft(Tables.Changefeed)
      .on(_.parentId === _.id)
      .map(r => (r._1, r._2.map(_.maxAck).ifNull(Tables.ChangeHistory.map(_.seq).max).ifNull(0L)))
      .result
      .headOption
  }).andThen(FutureUtil.logFailure("ChangefeedRepo.get"))

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
