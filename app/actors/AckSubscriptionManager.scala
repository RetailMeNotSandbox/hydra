package actors

import javax.inject.{Inject, Singleton}

import actors.ChangeHistoryListener.ChangeHistoryMaxUpdated
import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, OneForOneStrategy, Props, Stash, SupervisorStrategy, Terminated}
import akka.pattern.pipe
import com.github.mauricio.async.db.{Connection, QueryResult, Configuration => PGConfig}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.messages.backend.NotificationResponse
import com.github.mauricio.async.db.postgresql.util.URLParser
import com.google.inject.ImplementedBy
import core.util.FutureUtil
import play.Configuration

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

@ImplementedBy(classOf[DIChangefeedListenerBuilder])
trait ChangefeedListenerBuilder {
  def build(factory: ActorRefFactory) : ActorRef
}

class DIChangefeedListenerBuilder @Inject()(builder: AsyncPGBuilder) extends ChangefeedListenerBuilder {
  def build(factory: ActorRefFactory) : ActorRef = factory.actorOf(ChangefeedListener.props(builder))
}

@ImplementedBy(classOf[DIChangeHistoryListenerBuilder])
trait ChangeHistoryListenerBuilder {
  def build(factory: ActorRefFactory) : ActorRef
}

class DIChangeHistoryListenerBuilder @Inject() (builder: AsyncPGBuilder) extends ChangeHistoryListenerBuilder {
  def build(factory: ActorRefFactory) : ActorRef = factory.actorOf(ChangeHistoryListener.props(builder))
}

object AckSubscriptionManager {
  case class Subscribe(changefeedId: Option[String])
  case class Subscribed(changefeedId: Option[String])
  case class SubscriptionUpdate(changefeedId: Option[String], newAck: Long)
  case class SubscriptionError(changefeedId: Option[String], message: String)
  case class Unsubscribe(changefeedId: Option[String])
}

object ManagerMessages {
  case object CheckLiveliness
  case class Liveliness(alive: Boolean)
}

@Singleton()
class AckSubscriptionManager @Inject() (feedBuilder: ChangefeedListenerBuilder, historyBuilder: ChangeHistoryListenerBuilder) extends Actor with ActorLogging {
  import AckSubscriptionManager._
  import ManagerMessages._
  import ChangefeedListener._
  import context._

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, 1.minute)(SupervisorStrategy.defaultDecider)
  val feedListener = feedBuilder.build(context)
  context.watch(feedListener)
  var feedLiveliness = true
  val historyListener = historyBuilder.build(context)
  context.watch(historyListener)
  var historyLiveliness = true

  case class Listeners(listeners: Set[ActorRef]) {
    def ensure(listener: ActorRef) = this.copy(listeners + listener)
    def without(listener: ActorRef) = this.copy(listeners - listener)
    def merge(other: Listeners) = this.copy(listeners ++ other.listeners)
  }
  var activeSubscriptions : Map[String, Listeners] = Map()
  var pendingSubscriptions: Map[String, Listeners] = Map()
  var changeHistorySubscriptions: Set[ActorRef] = Set()

  def channelName(changefeedId: String) = s"changefeed_$changefeedId"
  def fromChannelName(channel: String) = channel.substring("changefeed_".length())

  def receive: Receive = {
    case CheckLiveliness => sender ! Liveliness(feedLiveliness && historyLiveliness)
    case Terminated(`feedListener`) => feedLiveliness = false
    case Terminated(`historyListener`) => historyLiveliness = false

    case Subscribe(Some(changefeedId)) =>
      val channel = channelName(changefeedId)
      activeSubscriptions.get(channel) match {
        case None =>
          if (pendingSubscriptions.contains(channel)) {
            pendingSubscriptions += (channel -> pendingSubscriptions(channel).ensure(sender()))
          } else {
            pendingSubscriptions += (channel -> Listeners(Set(sender())))
            feedListener ! ListenChannels(activeSubscriptions.keySet ++ pendingSubscriptions.keySet)
          }
        case Some(listeners) =>
          activeSubscriptions += (channel -> listeners.ensure(sender()))
          sender ! Subscribed(Some(changefeedId))
      }

    case Subscribe(None) =>
      // We're always "subscribed" to change_history
      changeHistorySubscriptions += sender
      sender ! Subscribed(None)

    case Unsubscribe(Some(changefeedId)) =>
      val channel = channelName(changefeedId)
      activeSubscriptions.get(channel) match {
        case None =>
          if (pendingSubscriptions.contains(channel)) {
            val newListeners = pendingSubscriptions(channel).without(sender())
            if (newListeners.listeners.isEmpty) {
              pendingSubscriptions -= channel
              feedListener ! ListenChannels(activeSubscriptions.keySet ++ pendingSubscriptions.keySet)
            } else {
              pendingSubscriptions += (channel -> newListeners)
            }
          }

        case Some(listeners) =>
          val newListeners = listeners.without(sender())
          if (newListeners.listeners.isEmpty) {
            activeSubscriptions -= channel
            feedListener ! ListenChannels(activeSubscriptions.keySet ++ pendingSubscriptions.keySet)
          } else {
            activeSubscriptions += (channel -> newListeners)
          }
      }

    case Unsubscribe(None) =>
      // since we're always "subscribed" to change_history even if there are no active listeners, do nothing
      changeHistorySubscriptions -= sender

    case ChannelsChanged(allChannels) =>
      pendingSubscriptions.filterKeys(allChannels.contains).foreach { case (channel, listeners) =>
          pendingSubscriptions -= channel
          activeSubscriptions += (channel -> activeSubscriptions.getOrElse(channel, Listeners(Set.empty)).merge(listeners))
          listeners.listeners.foreach { ref =>
            ref ! Subscribed(Some(fromChannelName(channel)))
          }
      }
      activeSubscriptions.filterKeys(k => !allChannels.contains(k)).foreach { case (channel, listeners) =>
          activeSubscriptions -= channel
          listeners.listeners.foreach { ref =>
            ref ! SubscriptionError(Some(fromChannelName(channel)), "Database channel closed unexpectedly")
          }
      }

    case n: NotificationResponse =>
      val listeners = activeSubscriptions.get(n.channel).map(_.listeners).getOrElse(Set.empty)
      if (n.payload == "deleted") {
        listeners.foreach { _ ! SubscriptionError(Some(fromChannelName(n.channel)), "Changefeed was deleted") }
        activeSubscriptions -= n.channel
        pendingSubscriptions -= n.channel
        feedListener ! ListenChannels(activeSubscriptions.keySet ++ pendingSubscriptions.keySet)
      } else {
        listeners.foreach { _ ! SubscriptionUpdate(Some(fromChannelName(n.channel)), n.payload.toLong)}
      }

    case ChangeHistoryMaxUpdated(max) =>
      changeHistorySubscriptions.foreach { _ ! SubscriptionUpdate(None, max) }
  }
}

object ChangefeedListener {
  def props(builder: AsyncPGBuilder) : Props = Props(new ChangefeedListener(builder))

  case class ListenChannels(channels: Set[String])
  case class ChannelsChanged(channels: Set[String])
  case class AddedChannel(channel: String)
  case class RemovedChannel(channel: String)
}

trait PostgresConnection {
  def disconnect : Future[_]
  def connect : Future[_]
  def registerNotifyListener(listener: NotificationResponse => Unit) : Unit
  def sendQuery(query: String) : Future[QueryResult]
}

class AsyncPostgresConnection(config: Configuration) extends PostgresConnection {
  val connection : PostgreSQLConnection = {
    val dbConfig = config.getConfig("slick.dbs.default.db")
    val url = dbConfig.getString("url")
    val parsed = URLParser.parse(url)
    val user = dbConfig.getString("user")
    val password = dbConfig.getString("password")

     new PostgreSQLConnection(PGConfig(user, parsed.host, parsed.port, Some(password), parsed.database, parsed.ssl, parsed.charset))
  }

  def disconnect: Future[_] = connection.disconnect
  def connect: Future[_] = connection.connect
  def registerNotifyListener(listener: (NotificationResponse) => Unit): Unit = connection.registerNotifyListener(listener)
  def sendQuery(query: String): Future[QueryResult] = connection.sendQuery(query)
}

class ChangefeedListener(builder: AsyncPGBuilder) extends Actor with ActorLogging {
  import ChangefeedListener._
  import context._

  val connection = builder.build()
  override def postStop(): Unit = {
    connection.disconnect
  }

  Await.result(connection.connect, 10.seconds)
  connection.registerNotifyListener(msg => context.parent ! msg)

  var inflight = false
  var currentChannels : Set[String] = Set.empty
  var desiredChannels : Set[String] = Set.empty

  context.parent ! ChannelsChanged(currentChannels)

  def execute() : Unit = {
    if (!inflight) {
      val toListen = desiredChannels.diff(currentChannels).headOption
      val toUnlisten = currentChannels.diff(desiredChannels).headOption

      inflight = (toListen, toUnlisten) match {
        case (Some(channel), _) =>
          val quotedChannel = channel.replaceAll("\"", "\"\"")
          connection.sendQuery("LISTEN \"" + quotedChannel + "\"").map(_ => AddedChannel(channel)).andThen(FutureUtil.logFailure("ChangefeedListener.listen")).pipeTo(self)
          true
        case (_, Some(channel)) =>
          val quotedChannel = channel.replaceAll("\"", "\"\"")
          connection.sendQuery("UNLISTEN \"" + quotedChannel + "\"").map(_ => RemovedChannel(channel)).andThen(FutureUtil.logFailure("ChangefeedListener.unlisten")).pipeTo(self)
          true
        case _ =>
          false
      }
    }
  }

  def receive = {
    case ListenChannels(channels) =>
      desiredChannels = channels
      execute()

    case AddedChannel(channel) =>
      currentChannels += channel
      inflight = false
      context.parent ! ChannelsChanged(currentChannels)
      execute()

    case RemovedChannel(channel) =>
      currentChannels -= channel
      inflight = false
      context.parent ! ChannelsChanged(currentChannels)
      execute()

    case Failure(e) =>
      throw e

    case msg =>
      log.warning(s"Unexpected msg in ChangefeedListener: $msg")
  }
}

object ChangeHistoryListener {
  def props(builder: AsyncPGBuilder): Props = Props(new ChangeHistoryListener(builder))
  case object CheckMax
  case class ChangeHistoryMaxUpdated(max: Long)
}
class ChangeHistoryListener(builder: AsyncPGBuilder) extends Actor with ActorLogging {
  import ChangeHistoryListener._
  import context._

  val connection = builder.build()
  // todo: experiment with sending `CheckMax` after `PostgresChangeHistoryRepository.expand/compact`
  // maybe the low tick period of .25s is good enough?
  val cancelTicker = context.system.scheduler.schedule(250.millis, 250.millis, self, CheckMax)
  override def postStop(): Unit = {
    cancelTicker.cancel()
    connection.disconnect
  }
  Await.result(connection.connect, 10.seconds)

  var inflight = false
  var maxSeq = 0L

  def execute() : Unit = {
    if (!inflight) {
      inflight = true
      pipe(connection.sendQuery("SELECT max(seq) FROM change_history")).to(self)
    }
  }

  def receive = {
    case CheckMax =>
      execute()

    case r: QueryResult =>
      inflight = false
      val result = r.rows.get.head(0).asInstanceOf[Long]
      if (result > maxSeq) {
        maxSeq = result
        context.parent ! ChangeHistoryMaxUpdated(result)
      }

    case Failure(e) =>
      throw e

    case msg =>
      log.warning(s"Unexpected msg in ChangeHistoryListener: $msg")
  }
}

