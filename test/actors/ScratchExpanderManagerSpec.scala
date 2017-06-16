package actors

import actors.AckSubscriptionManager._
import actors.ChangefeedListener.{ChannelsChanged, ListenChannels}
import actors.ManagerMessages._
import akka.actor.{ActorRef, ActorRefFactory, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.github.mauricio.async.db.postgresql.messages.backend.NotificationResponse
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike}
import testutil.{ErrorActor, VirtualScheduler}

class ScratchExpanderManagerSpec extends TestKit(ActorSystem("testSystem", ConfigFactory.parseString(
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

  def mkBuilder = new ScratchExpanderBuilder {
    val probe = TestProbe()
    override def build(factory: ActorRefFactory) = probe.ref
  }

  override def beforeEach: Unit = {
    scheduler.reset()
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "CompactionManager" should {
    "be lively by default" in {
      val builder = mkBuilder
      val ref = system.actorOf(Props(new ScratchExpanderManager(builder)))

      ref ! CheckLiveliness

      expectMsg(Liveliness(true))
    }
    "be lively across child restarts" in {
      val builder = new ScratchExpanderBuilder {
        var ref: ActorRef = null

        def build(factory: ActorRefFactory) = {
          ref = factory.actorOf(Props[ErrorActor])
          ref
        }
      }
      val ref = system.actorOf(Props(new ScratchExpanderManager(builder)))

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
  }
}
