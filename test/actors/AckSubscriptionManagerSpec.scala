package actors

import actors.AckSubscriptionManager._
import actors.ChangeHistoryListener.ChangeHistoryMaxUpdated
import actors.ManagerMessages._
import actors.ChangefeedListener.{ChannelsChanged, ListenChannels}
import akka.actor.{ActorRef, ActorRefFactory, ActorSystem, Kill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.github.mauricio.async.db.postgresql.messages.backend.NotificationResponse
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike}
import testutil.{ErrorActor, VirtualScheduler}

import scala.concurrent.duration._

class AckSubscriptionManagerSpec extends TestKit(ActorSystem("testSystem", ConfigFactory.parseString(
  """
    |akka.scheduler.implementation = testutil.VirtualScheduler
    |akka.actor.default-dispatcher.type = akka.testkit.CallingThreadDispatcherConfigurator
  """.stripMargin).withFallback(ConfigFactory.load())))
  with WordSpecLike
  with MustMatchers
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with ImplicitSender {

  val scheduler = system.scheduler.asInstanceOf[VirtualScheduler]
  def virtualTime = scheduler.virtualTime

  def mkBuilder = new ChangefeedListenerBuilder {
    val probe = TestProbe()
    override def build(factory: ActorRefFactory) = probe.ref
  }
  def mkHistoryBuilder = new ChangeHistoryListenerBuilder {
    val probe = TestProbe()
    override def build(factory: ActorRefFactory): ActorRef = probe.ref
  }

  override def beforeEach: Unit = {
    scheduler.reset()
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "AckSubscriptionManager" should {
    "be lively by default" in {
      val builder = mkBuilder
      val historyBuilder = mkHistoryBuilder
      val ref = system.actorOf(Props(new AckSubscriptionManager(builder, historyBuilder)))

      ref ! CheckLiveliness

      expectMsg(Liveliness(true))
    }
    "be lively across child changefeedlistener restarts" in {
      val builder = new ChangefeedListenerBuilder {
        var ref : ActorRef = null
        def build(factory: ActorRefFactory) = {
          ref = factory.actorOf(Props[ErrorActor])
          ref
        }
      }
      val historyBuilder = mkHistoryBuilder
      val ref = system.actorOf(Props(new AckSubscriptionManager(builder, historyBuilder)))

      ref ! CheckLiveliness
      expectMsg(Liveliness(true))

      for (i <- 1 to 10) {
        builder.ref ! new IllegalArgumentException(s"restart $i")
        ref ! CheckLiveliness
        expectMsg(Liveliness(true))
      }

      // too many restarts, it just stops the child

      // unfortunately, we can't test the "within 1 minute" aspect, since the retry logic directly uses `System.nanoTime`,
      // and not our virtual scheduler

      builder.ref ! new IllegalArgumentException("restart 11")
      ref ! CheckLiveliness
      expectMsg(Liveliness(false))
    }

    "be lively across child changehistorylistener restarts" in {
      val builder = new ChangeHistoryListenerBuilder {
        var ref : ActorRef = null
        def build(factory: ActorRefFactory) = {
          ref = factory.actorOf(Props[ErrorActor])
          ref
        }
      }
      val listenerBuilder = mkBuilder
      val ref = system.actorOf(Props(new AckSubscriptionManager(listenerBuilder, builder)))

      ref ! CheckLiveliness
      expectMsg(Liveliness(true))

      for (i <- 1 to 10) {
        builder.ref ! new IllegalArgumentException(s"restart $i")
        ref ! CheckLiveliness
        expectMsg(Liveliness(true))
      }

      // too many restarts, it just stops the child

      // unfortunately, we can't test the "within 1 minute" aspect, since the retry logic directly uses `System.nanoTime`,
      // and not our virtual scheduler

      builder.ref ! new IllegalArgumentException("restart 11")
      ref ! CheckLiveliness
      expectMsg(Liveliness(false))
    }

    "send a subscribed message immediately when subscribing to the root feed" in {
      val builder = mkBuilder
      val historyBuilder = mkHistoryBuilder
      val ref = system.actorOf(Props(new AckSubscriptionManager(builder, historyBuilder)))
      val subscriber = TestProbe()

      ref ! Subscribe(None)
      expectMsg(Subscribed(None))

      subscriber.send(ref, Subscribe(None))
      subscriber.expectMsg(Subscribed(None))

      msgAvailable mustEqual false
      subscriber.msgAvailable mustEqual false

    }
    "send a subscribed message when a subscription is confirmed for a non-root feed" in {
      val builder = mkBuilder
      val historyBuilder = mkHistoryBuilder
      val ref = system.actorOf(Props(new AckSubscriptionManager(builder, historyBuilder)))
      val subscriber = TestProbe()

      ref ! Subscribe(Some("primary"))
      subscriber.send(ref, Subscribe(Some("primary")))

      msgAvailable mustEqual false
      subscriber.msgAvailable mustEqual false

      ref ! ChannelsChanged(Set("changefeed_primary"))

      expectMsg(Subscribed(Some("primary")))
      subscriber.expectMsg(Subscribed(Some("primary")))
    }

    "send a subscribed message immediately when a subscription is already open" in {
      val builder = mkBuilder
      val historyBuilder = mkHistoryBuilder
      val ref = system.actorOf(Props(new AckSubscriptionManager(builder, historyBuilder)))
      val subscriber = TestProbe()

      ref ! Subscribe(Some("primary"))

      msgAvailable mustEqual false

      ref ! ChannelsChanged(Set("changefeed_primary"))

      expectMsg(Subscribed(Some("primary")))

      subscriber.msgAvailable mustEqual false

      subscriber.send(ref, Subscribe(Some("primary")))
      subscriber.expectMsg(Subscribed(Some("primary")))
    }

    "forward a translated notification event to all subscribers" in {
      val builder = mkBuilder
      val historyBuilder = mkHistoryBuilder
      val ref = system.actorOf(Props(new AckSubscriptionManager(builder, historyBuilder)))
      val rootSubscriber = TestProbe()
      val primarySubscriber = TestProbe()

      ref ! Subscribe(Some("primary"))
      rootSubscriber.send(ref, Subscribe(None))

      msgAvailable mustEqual false
      rootSubscriber.expectMsg(Subscribed(None))
      primarySubscriber.msgAvailable mustEqual false

      ref ! ChannelsChanged(Set("changefeed_primary"))

      expectMsg(Subscribed(Some("primary")))
      rootSubscriber.msgAvailable mustEqual false
      primarySubscriber.msgAvailable mustEqual false

      ref ! new NotificationResponse(0, "changefeed_primary", "123")

      expectMsg(SubscriptionUpdate(Some("primary"), 123))
      rootSubscriber.msgAvailable mustEqual false
      primarySubscriber.msgAvailable mustEqual false

      ref ! ChangeHistoryMaxUpdated(321)

      msgAvailable mustEqual false
      rootSubscriber.expectMsg(SubscriptionUpdate(None, 321))
      primarySubscriber.msgAvailable mustEqual false


      primarySubscriber.send(ref, Subscribe(Some("primary")))
      primarySubscriber.expectMsg(Subscribed(Some("primary")))


      ref ! new NotificationResponse(0, "changefeed_primary", "124")

      expectMsg(SubscriptionUpdate(Some("primary"), 124))
      rootSubscriber.msgAvailable mustEqual false
      primarySubscriber.expectMsg(SubscriptionUpdate(Some("primary"), 124))
    }

    "unsubscribe an actor when they send an unsubscribe message" in {
      val builder = mkBuilder
      val historyBuilder = mkHistoryBuilder
      val ref = system.actorOf(Props(new AckSubscriptionManager(builder, historyBuilder)))

      ref ! Subscribe(None)
      expectMsg(Subscribed(None))

      ref ! ChangeHistoryMaxUpdated(123)
      expectMsg(SubscriptionUpdate(None, 123))

      ref ! Unsubscribe(None)
      msgAvailable mustEqual false

      ref ! ChangeHistoryMaxUpdated(124)
      msgAvailable mustEqual false
    }

    "unsubscribe an actor when they send an unsubscribe message before the subscription was confirmed" in {
      val builder = mkBuilder
      val historyBuilder = mkHistoryBuilder
      val ref = system.actorOf(Props(new AckSubscriptionManager(builder, historyBuilder)))

      ref ! Subscribe(Some("primary"))
      msgAvailable mustEqual false

      ref ! Unsubscribe(Some("primary"))
      msgAvailable mustEqual false

      ref ! ChannelsChanged(Set("changefeed_primary"))
      msgAvailable mustEqual false

      ref ! new NotificationResponse(0, "changefeed_primary", "123")
      msgAvailable mustEqual false
    }

    "send an error message if the changefeed was deleted" in {
      val builder = mkBuilder
      val historyBuilder = mkHistoryBuilder
      val ref = system.actorOf(Props(new AckSubscriptionManager(builder, historyBuilder)))

      ref ! Subscribe(Some("primary"))
      msgAvailable mustEqual false

      ref ! ChannelsChanged(Set("changefeed_primary"))
      expectMsg(Subscribed(Some("primary")))

      ref ! new NotificationResponse(0, "changefeed_primary", "deleted")
      expectMsg(SubscriptionError(Some("primary"), "Changefeed was deleted"))

      // and unsubscribe them (not behavior that should happen if the feed was actually deleted, but maybe there were events reordered on the wire)
      ref ! new NotificationResponse(0, "changefeed_primary", "123")
      msgAvailable mustEqual false
    }

    "send an error message if the subscription was unexpectedly closed" in {
      val builder = mkBuilder
      val historyBuilder = mkHistoryBuilder
      val ref = system.actorOf(Props(new AckSubscriptionManager(builder, historyBuilder)))

      ref ! Subscribe(Some("primary"))
      msgAvailable mustEqual false

      ref ! ChannelsChanged(Set("changefeed_primary"))
      expectMsg(Subscribed(Some("primary")))

      ref ! new NotificationResponse(0, "changefeed_primary", "123")
      expectMsg(SubscriptionUpdate(Some("primary"), 123))

      ref ! ChannelsChanged(Set())
      expectMsg(SubscriptionError(Some("primary"), "Database channel closed unexpectedly"))

      // and resubscribing works:
      ref ! Subscribe(Some("primary"))
      msgAvailable mustEqual false

      ref ! ChannelsChanged(Set("changefeed_primary"))
      expectMsg(Subscribed(Some("primary")))

      ref ! new NotificationResponse(0, "changefeed_primary", "123")
      expectMsg(SubscriptionUpdate(Some("primary"), 123))
    }

    "request a subscription be opened when sent a subscriber message" in {
      val builder = mkBuilder
      val historyBuilder = mkHistoryBuilder
      val ref = system.actorOf(Props(new AckSubscriptionManager(builder, historyBuilder)))

      ref ! Subscribe(Some("secondary"))
      msgAvailable mustEqual false

      builder.probe.expectMsg(ListenChannels(Set("changefeed_secondary")))

      ref ! Subscribe(Some("primary"))
      msgAvailable mustEqual false

      builder.probe.expectMsg(ListenChannels(Set("changefeed_secondary", "changefeed_primary")))

      ref ! ChannelsChanged(Set("changefeed_primary"))

      expectMsg(Subscribed(Some("primary")))

      // since set has no concept of order, we increment the set in this test to guarantee order of the subscribed
      // notifications
      ref ! ChannelsChanged(Set("changefeed_primary", "changefeed_secondary"))
      expectMsg(Subscribed(Some("secondary")))
      msgAvailable mustEqual false

      // eh... we're encroaching undefined behavior here by resubscribing an already subscribed
      // actor.  Future Maintainer: If this is failing in the future, feel free to update this
      // test to subscribe to primary again with a testprobe instead of the implicit sender
      ref ! Subscribe(Some("primary"))

      expectMsg(Subscribed(Some("primary")))

      // we don't re-request a subscription if we know one is already open!
      builder.probe.msgAvailable mustEqual false
    }

    "request a subscription be closed when all subscribers unsubscribe" in {
      val builder = mkBuilder
      val historyBuilder = mkHistoryBuilder
      val ref = system.actorOf(Props(new AckSubscriptionManager(builder, historyBuilder)))
      val subscriber = TestProbe()

      ref ! Subscribe(Some("secondary"))
      msgAvailable mustEqual false

      subscriber.send(ref, Subscribe(Some("secondary")))
      subscriber.msgAvailable mustEqual false

      builder.probe.expectMsg(ListenChannels(Set("changefeed_secondary")))
      builder.probe.msgAvailable mustEqual false

      ref ! ChannelsChanged(Set("changefeed_secondary"))

      expectMsg(Subscribed(Some("secondary")))
      subscriber.expectMsg(Subscribed(Some("secondary")))

      ref ! Unsubscribe(Some("secondary"))
      msgAvailable mustEqual false
      builder.probe.msgAvailable mustEqual false

      ref ! new NotificationResponse(0, "changefeed_secondary", "123")
      msgAvailable mustEqual false
      subscriber.expectMsg(SubscriptionUpdate(Some("secondary"), 123))

      builder.probe.msgAvailable mustEqual false

      subscriber.send(ref, Unsubscribe(Some("secondary")))
      subscriber.msgAvailable mustEqual false

      builder.probe.expectMsg(ListenChannels(Set.empty))
    }

    "request a subscription be closed when all pending subscribers unsubscribe" in {
      val builder = mkBuilder
      val historyBuilder = mkHistoryBuilder
      val ref = system.actorOf(Props(new AckSubscriptionManager(builder, historyBuilder)))
      val subscriber = TestProbe()

      ref ! Subscribe(Some("secondary"))
      msgAvailable mustEqual false

      subscriber.send(ref, Subscribe(Some("secondary")))
      subscriber.msgAvailable mustEqual false

      builder.probe.expectMsg(ListenChannels(Set("changefeed_secondary")))

      ref ! Unsubscribe(Some("secondary"))

      builder.probe.msgAvailable mustEqual false

      subscriber.send(ref, Unsubscribe(Some("secondary")))

      builder.probe.expectMsg(ListenChannels(Set.empty))
    }

    "request a subscription be closed when its changefeed is deleted" in {
      val builder = mkBuilder
      val historyBuilder = mkHistoryBuilder
      val ref = system.actorOf(Props(new AckSubscriptionManager(builder, historyBuilder)))
      val subscriber = TestProbe()

      ref ! Subscribe(Some("secondary"))
      msgAvailable mustEqual false

      subscriber.send(ref, Subscribe(Some("secondary")))
      subscriber.msgAvailable mustEqual false

      builder.probe.expectMsg(ListenChannels(Set("changefeed_secondary")))

      ref ! new NotificationResponse(0, "changefeed_secondary", "deleted")

      builder.probe.expectMsg(ListenChannels(Set.empty))
    }
  }
}
