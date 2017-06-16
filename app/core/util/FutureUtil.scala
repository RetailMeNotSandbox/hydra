package core.util

import play.api.Logger

import scala.concurrent.{ExecutionContext, Promise, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

object FutureUtil {
  def logFailure[A](identifier: String) : PartialFunction[Try[A], Unit] = {
    case Failure(r) => Logger.error(s"Failed future '$identifier'", r)
  }
}
