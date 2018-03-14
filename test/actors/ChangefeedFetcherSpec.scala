package actors

import actors.AckSubscriptionManager._
import actors.ChangefeedFetcher._
import actors.ChangefeedSource.{BufferedRows, FetchError}
import akka.actor.FSM.{CurrentState, SubscribeTransitionCallBack, Transition}
import akka.actor.{ActorSystem, MultiQueueHack, PoisonPill}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import dal.{ChangeHistoryRepository, ChangeHistoryRow, ChangefeedRepository, ChangefeedRow}
import org.joda.time.{DateTime, DateTimeZone, LocalDateTime}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike}
import testutil.VirtualScheduler

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

class ChangefeedFetcherSpec extends TestKit(ActorSystem("testSystem", ConfigFactory.parseString(
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

  case class FetchRequest(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long)

  override def beforeEach: Unit = {
    scheduler.reset()
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  object TestChangefeedRepository {
    def apply(listR: Future[Seq[(ChangefeedRow, Long, Option[DateTime])]] = Future.failed(new NotImplementedError()),
              getR: Future[Option[(ChangefeedRow, Long, Option[DateTime])]] = Future.failed(new NotImplementedError()),
              simpleGetR: Future[Option[ChangefeedRow]] = Future.failed(new NotImplementedError()),
              createR: Future[Int] = Future.failed(new NotImplementedError()),
              deleteR: Future[Int] = Future.failed(new NotImplementedError()),
              ackR: Future[Int] = Future.failed(new NotImplementedError())
             ): ChangefeedRepository =
      new ChangefeedRepository {
        def simpleGet(id: String) = simpleGetR

        def get(id: String) = getR

        def delete(id: String) = deleteR

        def list(page: Int, size: Int) = listR

        def create(id: String, typeFilter: Option[List[String]], parentId: Option[String]) = createR

        def ack(id: String, ack: Long) = ackR
      }
  }

  object TestChangeHistoryRepository {
    def apply(getR: Future[Seq[ChangeHistoryRow]] = Future.failed(new NotImplementedError())): ChangeHistoryRepository =
      new ChangeHistoryRepository {
        def compact = ???
        def expand() = ???
        override def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = getR
      }
  }

  val nowTimestamp = new LocalDateTime(2017, 2, 6, 17, 12, 48).toDateTime(DateTimeZone.UTC)

  "ChangefeedFetcher" should {
    "subscribe to notifications on start" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val history = TestChangeHistoryRepository()
      val feeds = TestChangefeedRepository()

      val ref = system.actorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))

      subMgr.expectMsg(Subscribe(Some("primary")))
      subMgr.expectMsg(Subscribe(None))
    }

    "unsubscribe to notifications on stop" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val history = TestChangeHistoryRepository()
      val feeds = TestChangefeedRepository()
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))

      subMgr.expectMsg(Subscribe(Some("primary")))
      subMgr.expectMsg(Subscribe(None))

      ref ! PoisonPill

      subMgr.expectMsg(Unsubscribe(Some("primary")))
      subMgr.expectMsg(Unsubscribe(None))
    }

    "stop from Subscribing if subscriptions are not confirmed within the timeout" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val history = TestChangeHistoryRepository()
      val feeds = TestChangefeedRepository()

      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      virtualTime.advance(10.seconds)

      deathwatch.expectTerminated(ref)
      parentProbe.expectMsg(FetchError("Could not subscribe to the changefeed within the timeout"))
    }

    "transition from Subscribing to Initializing and request latest acks when subscriptions are confirmed" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val history = TestChangeHistoryRepository()
      var requested = false
      val feeds = new ChangefeedRepository {
        def simpleGet(id: String) = ???

        def get(id: String) = {
          requested = true
          Promise[Option[(ChangefeedRow, Long, Option[DateTime])]].future
        }

        def delete(id: String) = ???

        def list(page: Int, size: Int) = ???

        def create(id: String, typeFilter: Option[List[String]], parentId: Option[String]) = ???

        def ack(id: String, ack: Long) = ???
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      requested mustEqual true
    }

    "transition from Subscribing to Initializing with acks from received subscription updates" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      var acksRequest: Promise[Option[(ChangefeedRow, Long, Option[DateTime])]] = null
      val feeds = new ChangefeedRepository {
        def simpleGet(id: String) = ???

        def get(id: String) = {
          acksRequest = Promise[Option[(ChangefeedRow, Long, Option[DateTime])]]
          acksRequest.future
        }

        def delete(id: String) = ???

        def list(page: Int, size: Int) = ???

        def create(id: String, typeFilter: Option[List[String]], parentId: Option[String]) = ???

        def ack(id: String, ack: Long) = ???
      }
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! SubscriptionUpdate(Some("primary"), 101)
      monitor.msgAvailable mustEqual false

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      monitor.msgAvailable mustEqual false

      acksRequest.success(Some((changefeed, 102, None)))

      lastFetch mustEqual Some(FetchRequest(101, 102, None, 10))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))
    }

    "transition from Subscribing to Initializing with acks from rapidly received subscription updates" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      var acksRequest: Promise[Option[(ChangefeedRow, Long, Option[DateTime])]] = null
      val feeds = new ChangefeedRepository {
        def simpleGet(id: String) = ???

        def get(id: String) = {
          acksRequest = Promise[Option[(ChangefeedRow, Long, Option[DateTime])]]
          acksRequest.future
        }

        def delete(id: String) = ???

        def list(page: Int, size: Int) = ???

        def create(id: String, typeFilter: Option[List[String]], parentId: Option[String]) = ???

        def ack(id: String, ack: Long) = ???
      }
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! SubscriptionUpdate(Some("primary"), 101)
      monitor.msgAvailable mustEqual false

      // Dirty hack to simulate receiving another message before the actor has finished processing the current message.
      // This happens easily when actors are in a multithreaded environment, but our callingthreaddispatcher for tests
      // is single-threaded.
      MultiQueueHack.enqueueDirectly(self, ref, system, Subscribed(None))

      // A normal "send" to kick off processing of messages again
      ref ! SubscriptionUpdate(Some("primary"), 102)

      // What just happened here?

      // In effect, the ChangefeedFetcher got the second "Subscribed" notification, and while it was processing that
      // message (but before it sent itself the "SubscriptionsConfirmed" message), another "SubscriptionUpdate" message
      // was placed in the actors mailbox queue.

      // Before API-3489, this caused another Subscribing->Subscribing transition, which in turn caused a second
      // "SubscriptionsConfirmed" message to be enqueued.  Once the fetcher processed the first "SubscriptionsConfirmed"
      // message, we transition to Initializing, and the second outstanding "SubscriptionConfirmed" message caused
      // "unhandled event" error.

      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      monitor.msgAvailable mustEqual false

      acksRequest.success(Some((changefeed, 103, None)))

      lastFetch mustEqual Some(FetchRequest(102, 103, None, 10))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))
    }

    "stop from Initializing if latest acks couldn't be found" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val history = TestChangeHistoryRepository()
      val feeds = TestChangefeedRepository(getR = Future.successful(None))
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      deathwatch.expectTerminated(ref)
      parentProbe.expectMsg(FetchError("Changefeed was deleted during initialization phase"))
    }

    "stop from Initializing if there was an error retrieving lastAcks" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val history = TestChangeHistoryRepository()
      val feeds = TestChangefeedRepository()

      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      deathwatch.expectTerminated(ref)
      parentProbe.expectMsg(FetchError("Error while initializing: java.util.concurrent.ExecutionException: Boxed Error"))
    }

    "stop from Initializing if latestAcks are not retrieved within the timeout" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val history = TestChangeHistoryRepository()
      val feeds = TestChangefeedRepository(getR = Promise[Option[(ChangefeedRow, Long, Option[DateTime])]].future)

      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      deathwatch.msgAvailable mustEqual false

      virtualTime.advance(10.seconds)

      deathwatch.expectTerminated(ref)
      parentProbe.expectMsg(FetchError("Could not initialize the changefeed within the timeout"))
    }

    "stop from Initializing if a SubscriptionError is encountered while the lastAcks query is outstanding" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val history = TestChangeHistoryRepository()
      val feeds = TestChangefeedRepository(getR = Promise[Option[(ChangefeedRow, Long, Option[DateTime])]].future)

      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      deathwatch.msgAvailable mustEqual false

      ref ! SubscriptionError(None, "failed")

      deathwatch.expectTerminated(ref)
      parentProbe.expectMsg(FetchError("failed"))
    }

    "transition from Initializing to Fetching once latestAcks are retrieved" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 10))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))
    }

    "transition from Initializing to AtHead if latestAcks say the changefeed is already caught up to parent" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed.copy(maxAck = 20), 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual None
      monitor.expectMsg(Transition(ref, Initializing, AtHead))
    }

    "transition from Initializing to Fetching using ack values from SubscriptionUpdates while lastAck retrieval is outstanding" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      var acksRequest: Promise[Option[(ChangefeedRow, Long, Option[DateTime])]] = null
      val feeds = new ChangefeedRepository {
        def simpleGet(id: String) = ???

        def get(id: String) = {
          acksRequest = Promise[Option[(ChangefeedRow, Long, Option[DateTime])]]
          acksRequest.future
        }

        def delete(id: String) = ???

        def list(page: Int, size: Int) = ???

        def create(id: String, typeFilter: Option[List[String]], parentId: Option[String]) = ???

        def ack(id: String, ack: Long) = ???
      }
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      monitor.msgAvailable mustEqual false

      ref ! SubscriptionUpdate(Some("primary"), 1)
      monitor.expectMsg(Transition(ref, Initializing, Initializing))
      ref ! SubscriptionUpdate(None, 21)
      monitor.expectMsg(Transition(ref, Initializing, Initializing))

      acksRequest.success(Some((changefeed, 20, None)))

      lastFetch mustEqual Some(FetchRequest(1, 21, None, 10))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))
    }

    "stop from Fetching if a subscription error is encountered" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 10))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      ref ! SubscriptionError(None, "failure")

      deathwatch.expectTerminated(ref)
      parentProbe.expectMsg(FetchError("failure"))
    }

    "stop from Fetching if there is an error with the fetch query" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 10))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      fetchRequest.failure(new NotImplementedError("failure"))

      deathwatch.expectTerminated(ref)

      // since this is killed by the "whenUnhandled" catcher, we know it should work for all possible states
      parentProbe.expectMsg(FetchError("Received unhandled event Failure(java.util.concurrent.ExecutionException: Boxed Error) in state Fetching/BufferedSubscriptions(0,0,20,Set())"))
    }

    "transition from Fetching to AtHead if the fetch catches up to parent ack" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 10))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      fetchRequest.success(Seq(ChangeHistoryRow("foo", "bar", 20, now)))

      monitor.expectMsg(Transition(ref, Fetching, AtHead))
      parentProbe.expectMsg(BufferedRows(Seq(ChangeHistoryRow("foo", "bar", 20, now))))
    }

    "transition from Fetching to AtHead if this fetch returns fewer than the number of rows requested" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 10))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val results = Seq(ChangeHistoryRow("foo", "bar", 18, now))
      fetchRequest.success(results)

      monitor.expectMsg(Transition(ref, Fetching, AtHead))
      parentProbe.expectMsg(BufferedRows(results))
    }

    "transition from Fetching to AtHead if this fetch returns no rows" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 10))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val results = Seq.empty[ChangeHistoryRow]
      fetchRequest.success(results)

      monitor.expectMsg(Transition(ref, Fetching, AtHead))
      parentProbe.expectMsg(BufferedRows(results))
    }

    "transition from Fetching to BufferFull if this fetch fills the buffer" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 2, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 2))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val results = Seq(ChangeHistoryRow("foo", "bizz", 18, now), ChangeHistoryRow("foo", "bar", 19, now))
      fetchRequest.success(results)

      monitor.expectMsg(Transition(ref, Fetching, BufferFull))
      parentProbe.expectMsg(BufferedRows(results))
    }

    "transition from Fetching to BufferFull if the buffer is full, even if this fetch catches up to parent ack" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 2, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 2))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val results = Seq(ChangeHistoryRow("foo", "bizz", 19, now), ChangeHistoryRow("foo", "bar", 20, now))
      fetchRequest.success(results)

      monitor.expectMsg(Transition(ref, Fetching, BufferFull))
      parentProbe.expectMsg(BufferedRows(results))
    }

    "transition from Fetching to Fetching when this fetch completes if a parentAck came in and we still have room in the buffer" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 10))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      ref ! SubscriptionUpdate(None, 21)
      monitor.msgAvailable mustEqual false
      lastFetch mustEqual Some(FetchRequest(0, 20, None, 10))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val results = Seq(ChangeHistoryRow("foo", "bizz", 19, now), ChangeHistoryRow("foo", "bar", 20, now))
      fetchRequest.success(results)

      monitor.expectMsg(Transition(ref, Fetching, Fetching))
      lastFetch mustEqual Some(FetchRequest(20, 21, None, 8))
    }

    "transition from Fetching to Fetching when this fetch completes if an ack came in that cleared enough room in the buffer" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 2, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 2))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val results = Seq(ChangeHistoryRow("foo", "bizz", 1, now), ChangeHistoryRow("foo", "bar", 2, now))
      fetchRequest.success(results)

      monitor.expectMsg(Transition(ref, Fetching, BufferFull))
      parentProbe.expectMsg(BufferedRows(results))

      ref ! SubscriptionUpdate(Some("primary"), 1)

      monitor.expectMsg(Transition(ref, BufferFull, Fetching))
      lastFetch mustEqual Some(FetchRequest(2, 20, None, 1))

      ref ! SubscriptionUpdate(Some("primary"), 2)
      monitor.msgAvailable mustEqual false

      val secondResult = Seq(ChangeHistoryRow("foo", "hello", 3, now))
      fetchRequest.success(secondResult)

      monitor.expectMsg(Transition(ref, Fetching, Fetching))
      parentProbe.expectMsg(BufferedRows(secondResult))
      lastFetch mustEqual Some(FetchRequest(3, 20, None, 1))
    }

    "stop from Fetching if we receive a too high ack value while fetching" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 2, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 2))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      ref ! SubscriptionUpdate(Some("primary"), 5)

      deathwatch.expectTerminated(ref)
      parentProbe.expectMsg(FetchError("Received an ack for seqNum=5, but this was not a seqNum that had been sent (Ack'd up to 0, Sent Set())"))
    }

    "stop from Fetching if we receive an ack for a seq we didn't send" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 2, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 2))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val results = Seq(ChangeHistoryRow("foo", "bizz", 1, now), ChangeHistoryRow("foo", "bar", 3, now))
      fetchRequest.success(results)

      monitor.expectMsg(Transition(ref, Fetching, BufferFull))
      parentProbe.expectMsg(BufferedRows(results))

      ref ! SubscriptionUpdate(Some("primary"), 1)

      monitor.expectMsg(Transition(ref, BufferFull, Fetching))
      lastFetch mustEqual Some(FetchRequest(3, 20, None, 1))

      ref ! SubscriptionUpdate(Some("primary"), 2)
      deathwatch.expectTerminated(ref)
      parentProbe.expectMsg(FetchError("Received an ack for seqNum=2, but this was not a seqNum that had been sent (Ack'd up to 1, Sent Set(3))"))
    }

    "in Fetching, ignore stale acks" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 2, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 2))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val results = Seq(ChangeHistoryRow("foo", "bizz", 1, now), ChangeHistoryRow("foo", "bar", 3, now))
      fetchRequest.success(results)

      monitor.expectMsg(Transition(ref, Fetching, BufferFull))
      parentProbe.expectMsg(BufferedRows(results))

      ref ! SubscriptionUpdate(Some("primary"), 1)

      monitor.expectMsg(Transition(ref, BufferFull, Fetching))
      lastFetch mustEqual Some(FetchRequest(3, 20, None, 1))

      // ack so we go fetching -> fetching
      ref ! SubscriptionUpdate(Some("primary"), 3)

      ref ! SubscriptionUpdate(Some("primary"), 0)
      deathwatch.msgAvailable mustEqual false
      parentProbe.msgAvailable mustEqual false
      monitor.msgAvailable mustEqual false

      val secondResults = Seq(ChangeHistoryRow("foo", "hello", 4, now))
      fetchRequest.success(secondResults)

      monitor.expectMsg(Transition(ref, Fetching, Fetching))
      parentProbe.expectMsg(BufferedRows(secondResults))
      lastFetch mustEqual Some(FetchRequest(4, 20, None, 1))
    }

    "in Fetching, ignore stale parentAcks" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 2, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 2))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      ref ! SubscriptionUpdate(None, 0)

      deathwatch.msgAvailable mustEqual false
      parentProbe.msgAvailable mustEqual false
      monitor.msgAvailable mustEqual false

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val results = Seq(ChangeHistoryRow("foo", "bizz", 1, now), ChangeHistoryRow("foo", "bar", 3, now))
      fetchRequest.success(results)

      monitor.expectMsg(Transition(ref, Fetching, BufferFull))
      parentProbe.expectMsg(BufferedRows(results))

      ref ! SubscriptionUpdate(Some("primary"), 3)

      monitor.expectMsg(Transition(ref, BufferFull, Fetching))
      lastFetch mustEqual Some(FetchRequest(3, 20, None, 2))
    }

    "stop from BufferFull on subscriptionerror" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 2, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 2))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val firstResults = Seq(ChangeHistoryRow("foo", "bizz", 19, now), ChangeHistoryRow("foo", "bar", 20, now))
      fetchRequest.success(firstResults)

      monitor.expectMsg(Transition(ref, Fetching, BufferFull))
      parentProbe.expectMsg(BufferedRows(firstResults))

      ref ! SubscriptionError(None, "failure")

      deathwatch.expectTerminated(ref)
      parentProbe.expectMsg(FetchError("failure"))
    }

    "stop from BufferFull if we receive an ack that we didn't send" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 2, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 2))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val firstResults = Seq(ChangeHistoryRow("foo", "bizz", 18, now), ChangeHistoryRow("foo", "bar", 20, now))
      fetchRequest.success(firstResults)

      monitor.expectMsg(Transition(ref, Fetching, BufferFull))
      parentProbe.expectMsg(BufferedRows(firstResults))

      ref ! SubscriptionUpdate(Some("primary"), 1)

      deathwatch.expectTerminated(ref)
      parentProbe.expectMsg(FetchError("Received an ack for seqNum=1, but this was not a seqNum that had been sent (Ack'd up to 0, Sent Set(18, 20))"))
    }

    "in BufferFull, ignore stale acks" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed.copy(maxAck = 10), 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 2, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(10, 20, None, 2))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val firstResults = Seq(ChangeHistoryRow("foo", "bizz", 11, now), ChangeHistoryRow("foo", "bar", 12, now))
      fetchRequest.success(firstResults)

      monitor.expectMsg(Transition(ref, Fetching, BufferFull))
      parentProbe.expectMsg(BufferedRows(firstResults))

      ref ! SubscriptionUpdate(Some("primary"), 2)

      deathwatch.msgAvailable mustEqual false
      parentProbe.msgAvailable mustEqual false
      monitor.msgAvailable mustEqual false

      ref ! SubscriptionUpdate(Some("primary"), 12)

      monitor.expectMsg(Transition(ref, BufferFull, Fetching))
      lastFetch mustEqual Some(FetchRequest(12, 20, None, 2))
    }

    "in BufferFull, ignore stale parentAcks" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 2, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 2))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val firstResults = Seq(ChangeHistoryRow("foo", "bizz", 1, now), ChangeHistoryRow("foo", "bar", 2, now))
      fetchRequest.success(firstResults)

      monitor.expectMsg(Transition(ref, Fetching, BufferFull))
      parentProbe.expectMsg(BufferedRows(firstResults))

      ref ! SubscriptionUpdate(None, 1)

      deathwatch.msgAvailable mustEqual false
      parentProbe.msgAvailable mustEqual false
      monitor.msgAvailable mustEqual false

      ref ! SubscriptionUpdate(Some("primary"), 2)

      monitor.expectMsg(Transition(ref, BufferFull, Fetching))
      lastFetch mustEqual Some(FetchRequest(2, 20, None, 2))
    }

    "transition from BufferFull to Fetching on relevant ack" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 2, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 2))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val firstResults = Seq(ChangeHistoryRow("foo", "bizz", 1, now), ChangeHistoryRow("foo", "bar", 2, now))
      fetchRequest.success(firstResults)

      monitor.expectMsg(Transition(ref, Fetching, BufferFull))
      parentProbe.expectMsg(BufferedRows(firstResults))

      ref ! SubscriptionUpdate(Some("primary"), 2)

      monitor.expectMsg(Transition(ref, BufferFull, Fetching))
      lastFetch mustEqual Some(FetchRequest(2, 20, None, 2))
    }

    "transition from BufferFull to Fetching on relevant ack, using a parentAck that arrived in the meantime" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 2, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 2))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val firstResults = Seq(ChangeHistoryRow("foo", "bizz", 1, now), ChangeHistoryRow("foo", "bar", 2, now))
      fetchRequest.success(firstResults)

      monitor.expectMsg(Transition(ref, Fetching, BufferFull))
      parentProbe.expectMsg(BufferedRows(firstResults))

      ref ! SubscriptionUpdate(None, 30)
      ref ! SubscriptionUpdate(Some("primary"), 2)

      monitor.expectMsg(Transition(ref, BufferFull, Fetching))
      lastFetch mustEqual Some(FetchRequest(2, 30, None, 2))
    }

    "transition from BufferFull to AtHead if we receive an ack that clears some of the buffer but we're already caught up to the parent ack" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 2, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 2))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val firstResults = Seq(ChangeHistoryRow("foo", "bizz", 19, now), ChangeHistoryRow("foo", "bar", 20, now))
      fetchRequest.success(firstResults)

      monitor.expectMsg(Transition(ref, Fetching, BufferFull))
      parentProbe.expectMsg(BufferedRows(firstResults))

      ref ! SubscriptionUpdate(Some("primary"), 19)

      monitor.expectMsg(Transition(ref, BufferFull, AtHead))
      // same request as before, since there's no need to catch up
      lastFetch mustEqual Some(FetchRequest(0, 20, None, 2))
    }

    "stop from AtHead on subscription error" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 10))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val results = Seq(ChangeHistoryRow("foo", "bar", 18, now))
      fetchRequest.success(results)

      monitor.expectMsg(Transition(ref, Fetching, AtHead))
      parentProbe.expectMsg(BufferedRows(results))

      ref ! SubscriptionError(Some("primary"), "failure")

      deathwatch.expectTerminated(ref)
      parentProbe.expectMsg(FetchError("failure"))
    }

    "stop from AtHead if we receive an ack that we didn't send" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 10))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val results = Seq(ChangeHistoryRow("foo", "bar", 18, now))
      fetchRequest.success(results)

      monitor.expectMsg(Transition(ref, Fetching, AtHead))
      parentProbe.expectMsg(BufferedRows(results))

      ref ! SubscriptionUpdate(Some("primary"), 1)

      deathwatch.expectTerminated(ref)
      parentProbe.expectMsg(FetchError("Received an ack for seqNum=1, but this was not a seqNum that had been sent (Ack'd up to 0, Sent Set(18))"))
    }

    "in AtHead, ignore stale acks" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed.copy(maxAck = 10), 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(10, 20, None, 10))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val results = Seq(ChangeHistoryRow("foo", "bar", 18, now))
      fetchRequest.success(results)

      monitor.expectMsg(Transition(ref, Fetching, AtHead))
      parentProbe.expectMsg(BufferedRows(results))

      ref ! SubscriptionUpdate(Some("primary"), 1)

      deathwatch.msgAvailable mustEqual false
      monitor.msgAvailable mustEqual false
      parentProbe.msgAvailable mustEqual false

      ref ! SubscriptionUpdate(None, 21)

      lastFetch mustEqual Some(FetchRequest(20, 21, None, 9))
      monitor.expectMsg(Transition(ref, AtHead, Fetching))
    }

    "in AtHead, ignore stale parentAcks" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed, 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(0, 20, None, 10))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val results = Seq(ChangeHistoryRow("foo", "bar", 18, now))
      fetchRequest.success(results)

      monitor.expectMsg(Transition(ref, Fetching, AtHead))
      parentProbe.expectMsg(BufferedRows(results))

      ref ! SubscriptionUpdate(None, 1)

      deathwatch.msgAvailable mustEqual false
      monitor.msgAvailable mustEqual false
      parentProbe.msgAvailable mustEqual false

      ref ! SubscriptionUpdate(None, 21)

      lastFetch mustEqual Some(FetchRequest(20, 21, None, 9))
      monitor.expectMsg(Transition(ref, AtHead, Fetching))
    }

    "transition from AtHead to Fetching on a relevant parentAck" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed.copy(maxAck = 10), 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(10, 20, None, 10))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val results = Seq(ChangeHistoryRow("foo", "bar", 18, now))
      fetchRequest.success(results)

      monitor.expectMsg(Transition(ref, Fetching, AtHead))
      parentProbe.expectMsg(BufferedRows(results))

      ref ! SubscriptionUpdate(None, 21)

      lastFetch mustEqual Some(FetchRequest(20, 21, None, 9))
      monitor.expectMsg(Transition(ref, AtHead, Fetching))
    }

    "transition from AtHead to Fetching on a relevant parentAck, using an ack that arrived in the meantime" in {
      val changefeed = ChangefeedRow("primary", created = nowTimestamp, lastAck = nowTimestamp)
      val subMgr = TestProbe()
      val feeds = TestChangefeedRepository(getR = Future.successful(Some((changefeed.copy(maxAck = 10), 20, None))))
      var fetchRequest: Promise[Seq[ChangeHistoryRow]] = null
      var lastFetch: Option[FetchRequest] = None
      val history = new ChangeHistoryRepository {
        def expand() = ???
        def compact = ???
        def get(fromAck: Long, toAck: Long, typeFilter: Option[List[String]], size: Long) = {
          fetchRequest = Promise[Seq[ChangeHistoryRow]]
          lastFetch = Some(FetchRequest(fromAck, toAck, typeFilter, size))
          fetchRequest.future
        }
      }
      val parentProbe = TestProbe()
      val ref = parentProbe.childActorOf(ChangefeedFetcher.props(changefeed, subMgr.ref, history, feeds, 10, 10.seconds))
      val deathwatch = TestProbe()
      deathwatch.watch(ref)
      val monitor = TestProbe()
      ref ! SubscribeTransitionCallBack(monitor.ref)
      monitor.expectMsg(CurrentState(ref, Subscribing))

      ref ! Subscribed(Some("primary"))
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))

      ref ! Subscribed(None)
      monitor.expectMsg(Transition(ref, Subscribing, Subscribing))
      monitor.expectMsg(Transition(ref, Subscribing, Initializing))

      lastFetch mustEqual Some(FetchRequest(10, 20, None, 10))
      monitor.expectMsg(Transition(ref, Initializing, Fetching))

      val now = new LocalDateTime(2017, 9, 21, 17, 55, 12).toDateTime(DateTimeZone.UTC)
      val results = Seq(ChangeHistoryRow("foo", "bar", 18, now))
      fetchRequest.success(results)

      monitor.expectMsg(Transition(ref, Fetching, AtHead))
      parentProbe.expectMsg(BufferedRows(results))

      ref ! SubscriptionUpdate(Some("primary"), 18)

      deathwatch.msgAvailable mustEqual false
      monitor.msgAvailable mustEqual false
      parentProbe.msgAvailable mustEqual false

      ref ! SubscriptionUpdate(None, 21)

      lastFetch mustEqual Some(FetchRequest(20, 21, None, 10))
      monitor.expectMsg(Transition(ref, AtHead, Fetching))
    }
  }
}
