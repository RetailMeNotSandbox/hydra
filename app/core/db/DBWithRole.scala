package core.db

import core.util.Instrumented
import slick.backend.{DatabaseConfig, DatabasePublisher}
import slick.dbio.{DBIOAction, Effect, NoStream, Streaming}

import scala.concurrent.{ExecutionContext, Future}

class DBWithRole[R <: Role](name: String, databaseConfig: DatabaseConfig[_])(implicit ec: ExecutionContext) extends Instrumented {
  private val underlyingDatabase = databaseConfig.db

  val runTimer = timing("run", name)
  def run[A, E <: Effect](a: DBIOAction[A, NoStream, E])(implicit p: R HasPrivilege E): Future[A] = runTimer {
    underlyingDatabase.run(a)
  }
  def stream[A, E <: Effect](a: DBIOAction[_, Streaming[A], E])(implicit p: R HasPrivilege E): DatabasePublisher[A] = underlyingDatabase.stream(a)
}
