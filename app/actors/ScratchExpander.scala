package actors

import javax.inject.Inject

import akka.actor.Status.Failure
import akka.pattern.pipe
import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, OneForOneStrategy, Props, SupervisorStrategy, Terminated}
import com.google.inject.ImplementedBy
import core.util.Instrumented
import dal.ChangeHistoryRepository
import play.Configuration

import scala.concurrent.Await
import scala.concurrent.duration._

@ImplementedBy(classOf[DIAsyncPGBuilder])
trait AsyncPGBuilder {
  def build() : PostgresConnection
}

class DIAsyncPGBuilder @Inject() (config: Configuration) extends AsyncPGBuilder {
  def build() = new AsyncPostgresConnection(config)
}

@ImplementedBy(classOf[DIScratchExpanderBuilder])
trait ScratchExpanderBuilder {
  def build(factory: ActorRefFactory) : ActorRef
}

class DIScratchExpanderBuilder @Inject() (chr: ChangeHistoryRepository, config: Configuration) extends ScratchExpanderBuilder {
  def build(factory: ActorRefFactory) : ActorRef = factory.actorOf(ScratchExpander.props(chr, config.getInt("hydra.max-inflight.expander")))
}


class ScratchExpanderManager @Inject() (builder: ScratchExpanderBuilder) extends Actor with ActorLogging {
  import ManagerMessages._

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, 1.minute)(SupervisorStrategy.defaultDecider)
  val expander = builder.build(context)
  context.watch(expander)
  var liveliness = true

  def receive = {
    case CheckLiveliness => sender ! Liveliness(liveliness)
    case Terminated(`expander`) => liveliness = false
  }
}


object ScratchExpander {
  def props(chr: ChangeHistoryRepository, inflight: Int) = Props(new ScratchExpander(chr, inflight))
  case object TryExpand
  case class Expanded(batchSize: Int)
}
class ScratchExpander(chr: ChangeHistoryRepository, maxFlightCount: Int) extends Actor with ActorLogging with Instrumented {
  import ScratchExpander._
  import context._

  // todo: experiment with sending `TryExpand` after `PostgresResourceRepository.upsertResourceRows` as a replacement for the old LISTEN connection
  // maybe just lowering the tick period from 10s to .5s is enough?
  val cancelTicker = context.system.scheduler.schedule(500.millis, 500.millis, self, TryExpand)
  override def postStop(): Unit = {
    cancelTicker.cancel()
  }

  val inflightCounter = metrics.counter("inflight")
  var inFlight = 0

  def tryExpand() = {
    require(inFlight >= 0 && inFlight <= maxFlightCount)
    if (inFlight < maxFlightCount) {
      inflightCounter += 1
      inFlight += 1
      chr.expand().map(Expanded).pipeTo(self)
    }
  }

  def receive = {
    case TryExpand =>
      tryExpand()

    case Expanded(amount) =>
      inflightCounter -= 1
      inFlight -= 1
      if (amount > 0) {
        // double growth up to limit to 1) avoid initial stampede but still 2) ramp up to consume scratch
        tryExpand()
        tryExpand()
      }

    // `Failure` messages result from piping failed queries to ourselves
    case Failure(_) =>
      inflightCounter -= 1
      inFlight -= 1
  }
}
