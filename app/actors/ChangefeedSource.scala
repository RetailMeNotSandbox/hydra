package actors

import akka.actor.{Props, Terminated}
import akka.stream.actor.ActorPublisher
import dal.ChangeHistoryRow
import akka.actor.SupervisorStrategy._
import core.util.Instrumented

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

object ChangefeedSource {
  def props(timeout: FiniteDuration, fetcherBuilder: ActorBuilder): Props =
    Props(new ChangefeedSource(timeout, fetcherBuilder))

  case class ChangefeedEvent(eventType: String, data: Option[ChangeHistoryRow] = None, message: Option[String] = None)
  case class FetchError(message: String)
  case class BufferedRows(rows: Seq[ChangeHistoryRow])
}

class ChangefeedSource(timeout: FiniteDuration, fetcherBuilder: ActorBuilder)
  extends ActorPublisher[ChangefeedSource.ChangefeedEvent] with Instrumented {

  import actors.ChangefeedSource._
  import akka.stream.actor.ActorPublisherMessage._

  // implicit execution context
  import context._


  metrics.counter("activeChangefeeds").inc()
  override def postStop(): Unit = {
    super.postStop()
    metrics.counter("activeChangefeeds").dec()
  }


  var queue = mutable.Queue[ChangeHistoryRow]()

  def tryDeliver(): Unit =
    while (totalDemand > 0 && queue.nonEmpty) {
      onNext(ChangefeedEvent("event", data = Some(queue.dequeue())))
    }

  override def supervisorStrategy = stoppingStrategy
  val fetcher = fetcherBuilder(context)
  context.watch(fetcher)

  def receive = {
    case BufferedRows(rows) =>
      queue ++= rows
      tryDeliver()

    case Request(_) =>
      tryDeliver()

    case FetchError(message) =>
      // todo: there's probably a better way where we wait to stop until the error event has been submitted
      if (totalDemand > 0) {
        onNext(ChangefeedEvent("error", message = Some(message)))
      }
      context.stop(self)

    // special backticks here so that we match on the value stored within the `fetcher` variable, rather than creating a
    // new fetcher var that shadows the existing one
    case Terminated(`fetcher`) =>
      // todo: there's probably a better way where we wait to stop until the error event has been submitted
      if (totalDemand > 0) {
        onNext(ChangefeedEvent("error", message = Some("Fetcher terminated unexpectedly")))
      }
      context.stop(self)

    case Cancel =>
      context.stop(self)
  }

}


