package actors

import actors.AckSubscriptionManager._
import actors.ChangefeedSource.{BufferedRows, FetchError}
import akka.actor.FSM.Failure
import akka.actor.{ActorRef, FSM, Props}
import akka.pattern.{ask, pipe}
import dal.{ChangeHistoryRepository, ChangeHistoryRow, ChangefeedRepository, ChangefeedRow}

import scala.concurrent.duration.FiniteDuration

object ChangefeedFetcher {
  def props(changefeed: ChangefeedRow, subMgr: ActorRef, history: ChangeHistoryRepository, feeds: ChangefeedRepository, maxUnackedCount: Int, timeout: FiniteDuration): Props =
    Props(new ChangefeedFetcher(changefeed, subMgr, history, feeds, maxUnackedCount, timeout))

  // our events
  case class FetchResult(rows: Seq[ChangeHistoryRow], fromAck: Long, toAck: Long, maxSize: Int)
  case object SubscriptionsConfirmed
  case class InitialAcksConfirmed(maxAck: Long, parentMaxAck: Long)
  case object FetchStarted

  // our state
  sealed trait State
  case object Subscribing extends State
  case object Initializing extends State
  case object BufferFull extends State
  case object AtHead extends State
  case object Fetching extends State

  // our state data
  sealed trait Data
  final case class GatheredSubscriptions(thisChangefeed: Boolean, thisAck: Option[Long], parentChangefeed: Boolean, parentAck: Option[Long]) extends Data
  final case class InitialAcks(maxAck: Option[Long], parentMaxAck: Option[Long]) extends Data
  final case class BufferedSubscriptions(maxAck: Long, fromAck: Long, parentMaxAck: Long, inflight: Set[Long]) extends Data {
    require(maxAck <= fromAck)
    require(fromAck <= parentMaxAck)
    if (inflight.nonEmpty) {
      require(inflight.min > maxAck)
      require(inflight.max <= fromAck)
    }

    def ack(ack: Long): BufferedSubscriptions = {
      require(ack <= fromAck)
      if (ack <= maxAck) {
        this
      } else {
        this.copy(maxAck = ack, inflight = inflight.filter(_ > ack))
      }
    }
  }

}

// todo: there's probably some optimizations to be had in the atHead state when `changefeed.parentId == None` by
//       sharing a common fetcher
class ChangefeedFetcher(changefeed: ChangefeedRow, subMgr: ActorRef, history: ChangeHistoryRepository, feeds: ChangefeedRepository, maxUnackedCount: Int, timeout: FiniteDuration)
  extends FSM[ChangefeedFetcher.State, ChangefeedFetcher.Data] {
  require(maxUnackedCount > 0)
  import actors.ChangefeedFetcher._

  // implicit execution context
  import context._

  subMgr ! Subscribe(Some(changefeed.id))
  subMgr ! Subscribe(changefeed.parentId)

  override def postStop(): Unit = {
    super.postStop()
    subMgr ! Unsubscribe(Some(changefeed.id))
    subMgr ! Unsubscribe(changefeed.parentId)
  }

  def tryFetch(data: BufferedSubscriptions) : State = {
    val maxSize = maxUnackedCount - data.inflight.size

    // the greater part of "greater or equal" in these if statements should never be relevant,
    // but I included it here for safety
    if (0 >= maxSize) {
      goto(BufferFull) using data
    }
    else if (data.fromAck >= data.parentMaxAck) {
      goto(AtHead) using data
    }
    else {
      history.get(data.fromAck, data.parentMaxAck, changefeed.typeFilter, maxSize)
        .map(rows => FetchResult(rows, data.fromAck, data.parentMaxAck, maxSize))
        .pipeTo(self)

      goto(Fetching) using data
    }
  }

  def maybeMax(left: Long, maybeRight: Option[Long]) : Long = {
    maybeRight.map(r => Math.max(left, r)).getOrElse(left)
  }

  startWith(Subscribing, GatheredSubscriptions(thisChangefeed = false, thisAck = None, parentChangefeed = false, parentAck=None), Some(timeout))

  when(Subscribing, stateTimeout = timeout) {
    case Event(Subscribed(Some(changefeed.id)), x : GatheredSubscriptions) =>
      goto(Subscribing) using x.copy(thisChangefeed = true)

    case Event(Subscribed(changefeed.parentId), x : GatheredSubscriptions) =>
      goto(Subscribing) using x.copy(parentChangefeed = true)

    case Event(SubscriptionUpdate(Some(changefeed.id), newAck), x : GatheredSubscriptions) =>
      stay() using x.copy(thisAck = Some(newAck))

    case Event(SubscriptionUpdate(changefeed.parentId, newAck), x : GatheredSubscriptions) =>
      stay() using x.copy(parentAck = Some(newAck))

    case Event(SubscriptionsConfirmed, x: GatheredSubscriptions) =>
      goto(Initializing) using InitialAcks(x.thisAck, x.parentAck)

    case Event(StateTimeout, _) =>
      stop(Failure("Could not subscribe to the changefeed within the timeout"))
  }

  onTransition {
    case Subscribing -> Subscribing =>
      nextStateData match {
        case GatheredSubscriptions(true, _, true, _) =>
          self ! SubscriptionsConfirmed
        case _ =>
      }
  }

  when(Initializing, stateTimeout = timeout) {
    case Event(InitialAcksConfirmed(maxAck, parentMaxAck), data : InitialAcks) =>
      val actualMaxAck = maybeMax(maxAck, data.maxAck)

      tryFetch(BufferedSubscriptions(
        actualMaxAck,
        actualMaxAck,
        maybeMax(parentMaxAck, data.parentMaxAck),
        Set.empty
      ))

    case Event(SubscriptionUpdate(Some(changefeed.id), newAck), data: InitialAcks) =>
      goto(Initializing) using data.copy(maxAck = Some(maybeMax(newAck, data.maxAck)))

    case Event(SubscriptionUpdate(changefeed.parentId, newAck), data: InitialAcks) =>
      goto(Initializing) using data.copy(parentMaxAck = Some(maybeMax(newAck, data.parentMaxAck)))

    case Event(SubscriptionError(_, message), _) =>
      stop(Failure(message))

    case Event(StateTimeout, _) =>
      stop(Failure("Could not initialize the changefeed within the timeout"))
  }

  onTransition {
    case Subscribing -> Initializing =>
      // We only try to get the initial ack values after we've confirmed that we're subscribed, otherwise there's a period
      // of time after the initial retrieval where events could slip in before we'd get notified of them
      // Doing it in this order may result in listening to unnecessary events, but at least there's no missing time coverage
      feeds.get(changefeed.id).map {
        case Some((row, maxParentAck, _)) => InitialAcksConfirmed(row.maxAck, maxParentAck)
        case None => SubscriptionError(Some(changefeed.id), "Changefeed was deleted during initialization phase")
      }.recover {
        case err => SubscriptionError(Some(changefeed.id), s"Error while initializing: $err")
      }.pipeTo(self)

    // todo: bug here where we confirm the InitialAcks due to subscription updates, and the database query is still
    // outstanding.  Once we transition out of Initializing, the database result finally arrives and is treated
    // as an unexpected event, triggering the shutdown of the stream.
    //
    // As a workaround for now, let's just require that we wait until the database query sends the
    // `InitialAcksConfirmed` message instead of autogenerating one from SubscriptionUpdate events.

//    case Initializing -> Initializing =>
//      nextStateData match {
//        case InitialAcks(Some(maxAck), Some(parentMaxAck)) =>
//          self ! InitialAcksConfirmed(maxAck, parentMaxAck)
//        case _ =>
//      }
  }

  when(BufferFull) {
    case Event(SubscriptionUpdate(Some(changefeed.id), newAck), data: BufferedSubscriptions) =>
      if (newAck <= data.maxAck) {
        stay()
      } else if (data.inflight.contains(newAck)) {
        tryFetch(data.ack(newAck))
      } else {
        stop(Failure(s"Received an ack for seqNum=$newAck, but this was not a seqNum that had been sent (Ack'd up to ${data.maxAck}, Sent ${data.inflight})"))
      }

    case Event(SubscriptionUpdate(changefeed.parentId, newAck), data: BufferedSubscriptions) =>
      if (newAck > data.parentMaxAck) {
        stay() using data.copy(parentMaxAck = newAck)
      } else {
        stay()
      }

    case Event(SubscriptionError(_, message), _) =>
      stop(Failure(message))
  }

  when(AtHead) {
    case Event(SubscriptionUpdate(Some(changefeed.id), newAck), data: BufferedSubscriptions) =>
      if (newAck <= data.maxAck) {
        stay()
      } else if (data.inflight.contains(newAck)) {
        stay() using data.ack(newAck)
      } else {
        stop(Failure(s"Received an ack for seqNum=$newAck, but this was not a seqNum that had been sent (Ack'd up to ${data.maxAck}, Sent ${data.inflight})"))
      }

    case Event(SubscriptionUpdate(changefeed.parentId, newAck), data: BufferedSubscriptions) =>
      if (newAck > data.parentMaxAck) {
        tryFetch(data.copy(parentMaxAck = newAck))
      } else {
        stay()
      }

    case Event(SubscriptionError(_, message), _) =>
      stop(Failure(message))
  }

  when(Fetching) {
    case Event(FetchResult(rows, fromAck, toAck, maxSize), data: BufferedSubscriptions) =>
      require(maxSize > 0, "tryFetch should not have fetch if there is no room left in the buffer")

      context.parent ! BufferedRows(rows)

      // maxSize is the maximum number of rows this fetch could have returned.
      //
      // If it returned fewer rows than that, we can assume all change_history rows between the `rows.map(_.seq).max`
      // and `toAck` are not relevant to this changefeed's `typefilter`.
      //
      // As such, we should use either the max seq we retrieved or the upper bounds of this range when determining the lower
      // bounds of the next seq range.  This lets us avoid unnecessarily querying the same rows over and over again

      val returnedSeqs = rows.map(_.seq)

      val nextFrom = if (rows.size < maxSize) toAck else returnedSeqs.max

      tryFetch(data.copy(inflight = data.inflight ++ returnedSeqs, fromAck = nextFrom))

    case Event(SubscriptionUpdate(Some(changefeed.id), newAck), data: BufferedSubscriptions) =>
      if (newAck <= data.maxAck) {
        stay()
      } else if (data.inflight.contains(newAck)) {
        stay() using data.ack(newAck)
      } else {
        stop(Failure(s"Received an ack for seqNum=$newAck, but this was not a seqNum that had been sent (Ack'd up to ${data.maxAck}, Sent ${data.inflight})"))
      }

    case Event(SubscriptionUpdate(changefeed.parentId, newAck), data: BufferedSubscriptions) =>
      if (newAck > data.parentMaxAck) {
        stay() using data.copy(parentMaxAck = newAck)
      } else {
        stay()
      }

    case Event(SubscriptionError(_, message), _) =>
      stop(Failure(message))
  }

  onTermination {
    case StopEvent(FSM.Failure(cause), _, _) =>
      context.parent ! FetchError(cause.toString)
  }

  whenUnhandled {
    case Event(e, s) =>
      log.error("Received unhandled event {} in state {}/{}", e, stateName, s)
      stop(Failure(s"Received unhandled event $e in state $stateName/$s"))
  }

  initialize()
}


