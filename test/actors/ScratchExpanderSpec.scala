package actors

import actors.ManagerMessages._
import actors.ScratchExpander.TryExpand
import akka.actor.{ActorRef, ActorRefFactory, ActorSystem, Kill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.postgresql.messages.backend.NotificationResponse
import com.typesafe.config.ConfigFactory
import dal.{ChangeHistoryRepository, ChangeHistoryRow}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike}
import testutil.{ErrorActor, VirtualScheduler}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

class ScratchExpanderSpec extends TestKit(ActorSystem("testSystem", ConfigFactory.parseString(
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

  object TestChangeHistoryRepository {
    def apply(results: Option[Int] = None): ChangeHistoryRepository =
      new ChangeHistoryRepository {
        def expand = results.map(Future.successful).getOrElse(Future.failed(new NotImplementedError()))
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = ???
      }
  }

  override def beforeEach: Unit = {
    scheduler.reset()
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "ScratchExpander" should {
    "call expand when receiving a TryExpand message" in {
      var expandCount = 0
      val chr = new ChangeHistoryRepository {
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = ???
        var expandPromise = Promise[Int]()
        def expand() = {
          expandCount += 1
          expandPromise = Promise[Int]()
          expandPromise.future
        }
      }

      val ref = system.actorOf(ScratchExpander.props(chr, 2))

      expandCount mustEqual 0

      ref ! TryExpand

      expandCount mustEqual 1
    }
    "not call expand when receiving a TryExpand message if there are more than maxInflight expand attempts in flight" in {
      var expandCount = 0
      val chr = new ChangeHistoryRepository {
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = ???
        var expandPromise = Promise[Int]()
        def expand() = {
          expandCount += 1
          expandPromise = Promise[Int]()
          expandPromise.future
        }
      }
      val maxInflight = 16

      val ref = system.actorOf(ScratchExpander.props(chr, maxInflight))

      expandCount mustEqual 0

      for (_ <- 1 to maxInflight) {
        ref ! TryExpand
      }

      expandCount mustEqual maxInflight

      ref ! TryExpand

      expandCount mustEqual maxInflight
    }
    "receive a TryExpand message after .5 seconds" in {
      var expandCount = 0
      val chr = new ChangeHistoryRepository {
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = ???
        var expandPromise = Promise[Int]()
        def expand() = {
          expandCount += 1
          expandPromise = Promise[Int]()
          expandPromise.future
        }
      }

      val ref = system.actorOf(ScratchExpander.props(chr, 2))

      expandCount mustEqual 0

      virtualTime.advance(500.millis)

      expandCount mustEqual 1
    }
    "call expand twice if a previous expand call affected rows" in {
      var expandCount = 0
      val chr = new ChangeHistoryRepository {
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = ???
        var expandPromise = Promise[Int]()
        def expand() = {
          expandCount += 1
          expandPromise = Promise[Int]()
          expandPromise.future
        }
      }

      val ref = system.actorOf(ScratchExpander.props(chr, 2))

      expandCount mustEqual 0

      ref ! TryExpand

      expandCount mustEqual 1

      chr.expandPromise.success(1)

      expandCount mustEqual 3
    }
    "call expand twice if a previous expand call affected rows, but respsect max inflight" in {
      var expandCount = 0
      val chr = new ChangeHistoryRepository {
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = ???
        var expandPromise = Promise[Int]()
        def expand() = {
          expandCount += 1
          expandPromise = Promise[Int]()
          expandPromise.future
        }
      }
      val maxInflight = 16

      val ref = system.actorOf(ScratchExpander.props(chr, maxInflight))

      expandCount mustEqual 0

      for (_ <- 1 to maxInflight) {
        ref ! TryExpand
      }

      expandCount mustEqual maxInflight

      chr.expandPromise.success(1)
      // 15 still "busy", 1 completes, room for one more in flight

      expandCount mustEqual 17 //not 18
    }
    "not call expand if a previous expand call affected no rows" in {
      var expandCount = 0
      val chr = new ChangeHistoryRepository {
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = ???
        var expandPromise = Promise[Int]()
        def expand() = {
          expandCount += 1
          expandPromise = Promise[Int]()
          expandPromise.future
        }
      }

      val ref = system.actorOf(ScratchExpander.props(chr, 2))

      expandCount mustEqual 0

      ref ! TryExpand

      expandCount mustEqual 1

      chr.expandPromise.success(0)

      expandCount mustEqual 1
    }
    "decrement in flight count when a previous expand call finishes" in {
      var expandCount = 0
      val chr = new ChangeHistoryRepository {
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = ???
        var expandPromise = Promise[Int]()
        def expand() = {
          expandCount += 1
          expandPromise = Promise[Int]()
          expandPromise.future
        }
      }
      val maxInflight = 16

      val ref = system.actorOf(ScratchExpander.props(chr, maxInflight))

      expandCount mustEqual 0

      for (_ <- 1 to maxInflight) {
        ref ! TryExpand
      }

      expandCount mustEqual maxInflight

      ref ! TryExpand

      expandCount mustEqual maxInflight

      chr.expandPromise.success(0)

      expandCount mustEqual maxInflight

      ref ! TryExpand

      expandCount mustEqual 17
    }
    "decrement in flight count when a previous expand call fails" in {
      var expandCount = 0
      val chr = new ChangeHistoryRepository {
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = ???
        var expandPromise = Promise[Int]()
        def expand() = {
          expandCount += 1
          expandPromise = Promise[Int]()
          expandPromise.future
        }
      }

      val maxInflight = 16

      val ref = system.actorOf(ScratchExpander.props(chr, maxInflight))

      expandCount mustEqual 0

      for (_ <- 1 to maxInflight) {
        ref ! TryExpand
      }

      expandCount mustEqual maxInflight

      ref ! TryExpand

      expandCount mustEqual maxInflight

      chr.expandPromise.failure(new NotImplementedError("failure"))

      expandCount mustEqual maxInflight

      ref ! TryExpand

      expandCount mustEqual 17
    }
  }
}
