package actors

import actors.ChangefeedSource.{BufferedRows, ChangefeedEvent, FetchError}
import akka.actor.{ActorRef, ActorSystem, Kill, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import dal.ChangeHistoryRow
import org.joda.time.{DateTimeZone, LocalDateTime}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike}
import testutil.{ErrorActor, VirtualScheduler}

import scala.concurrent.duration._

class ChangefeedSourceSpec extends TestKit(ActorSystem("testSystem", ConfigFactory.parseString(
  """
    |akka.scheduler.implementation = testutil.VirtualScheduler
    |akka.actor.default-dispatcher.type = akka.testkit.CallingThreadDispatcherConfigurator
  """.stripMargin).withFallback(ConfigFactory.load())))
  with WordSpecLike
  with MustMatchers
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with ImplicitSender {

  implicit val mat = ActorMaterializer()(system)
  val scheduler = system.scheduler.asInstanceOf[VirtualScheduler]

  def virtualTime = scheduler.virtualTime

  override def beforeEach: Unit = {
    scheduler.reset()
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "ChangefeedSource" should {
    "emit from buffer when stream indicates demand" in {
      val sourceUnderTest = Source.actorPublisher[ChangefeedEvent](ChangefeedSource.props(10.seconds, (_) => TestProbe().ref))

      val (sourceRef, probe) = sourceUnderTest.toMat(TestSink.probe[ChangefeedEvent])(Keep.both).run()

      probe
        .ensureSubscription()
        .expectNoMsg(0.seconds)

      val now = new LocalDateTime(2017, 2, 6, 17, 12, 48).toDateTime(DateTimeZone.UTC)
      sourceRef ! BufferedRows(Seq(ChangeHistoryRow("foo", "bar", 1, now)))

      probe
        .expectNoMsg(0.seconds)
        .request(1)
        .expectNext(ChangefeedEvent("event", Some(ChangeHistoryRow("foo", "bar", 1, now))))
    }

    "shutdown on a fetch error" in {
      val sourceUnderTest = Source.actorPublisher[ChangefeedEvent](ChangefeedSource.props(10.seconds, (_) => TestProbe().ref))
      val (sourceRef, probe) = sourceUnderTest.toMat(TestSink.probe[ChangefeedEvent])(Keep.both).run()
      val deathWatch = TestProbe()
      deathWatch.watch(sourceRef)

      probe
        .ensureSubscription()
        .expectNoMsg(0.seconds)
        .request(1)


      sourceRef ! FetchError("some error")

      probe
        .expectNext(ChangefeedEvent("error", message = Some("some error")))
        .expectComplete()

      deathWatch.expectTerminated(sourceRef)
    }
    "shutdown if fetcher terminates" in {
      val fetcher = TestProbe()
      val sourceUnderTest = Source.actorPublisher[ChangefeedEvent](ChangefeedSource.props(10.seconds, (_) => fetcher.ref))

      val (sourceRef, probe) = sourceUnderTest.toMat(TestSink.probe[ChangefeedEvent])(Keep.both).run()
      val deathWatch = TestProbe()
      deathWatch.watch(sourceRef)

      probe
        .ensureSubscription()
        .expectNoMsg(0.seconds)
        .request(1)

      system.stop(fetcher.ref)

      probe
        .expectNext(ChangefeedEvent("error", message = Some("Fetcher terminated unexpectedly")))
        .expectComplete()

      deathWatch.expectTerminated(sourceRef)
    }
    "shutdown if fetcher throws an error" in {
      var fetcher : ActorRef = null
      val sourceUnderTest = Source.actorPublisher[ChangefeedEvent](ChangefeedSource.props(10.seconds, (factory) => {
        fetcher = factory.actorOf(Props[ErrorActor])
        fetcher
      }))

      val (sourceRef, probe) = sourceUnderTest.toMat(TestSink.probe[ChangefeedEvent])(Keep.both).run()
      val deathWatch = TestProbe()
      deathWatch.watch(sourceRef)

      probe
        .ensureSubscription()
        .expectNoMsg(0.seconds)
        .request(1)

      fetcher ! new IllegalArgumentException("exception")

      probe
        .expectNext(ChangefeedEvent("error", message = Some("Fetcher terminated unexpectedly")))
        .expectComplete()

      deathWatch.expectTerminated(sourceRef)
    }

    "shutdown on cancel" in {
      val sourceUnderTest = Source.actorPublisher[ChangefeedEvent](ChangefeedSource.props(10.seconds, (_) => TestProbe().ref))

      val (sourceRef, probe) = sourceUnderTest.toMat(TestSink.probe[ChangefeedEvent])(Keep.both).run()
      val deathWatch = TestProbe()
      deathWatch.watch(sourceRef)

      probe
        .ensureSubscription()
        .expectNoMsg(0.seconds)
        .cancel()

      deathWatch.expectTerminated(sourceRef)

    }
  }
}
