package actors

import actors.ChangefeedListener.{ChannelsChanged, ListenChannels}
import akka.actor.{ActorContext, ActorRef, ActorSystem, ChildRestartStats, Kill, SupervisorStrategy}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.postgresql.messages.backend.NotificationResponse
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike}
import testutil.VirtualScheduler

import scala.concurrent.{Future, Promise}

class ChangefeedListenerSpec extends TestKit(ActorSystem("testSystem", ConfigFactory.parseString(
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

  override def beforeEach: Unit = {
    scheduler.reset()
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  def connectionBuilder(conn: PostgresConnection) : AsyncPGBuilder = new AsyncPGBuilder {
    override def build() = conn
  }

  class TestPostgresConnection extends PostgresConnection {
    var listener : (NotificationResponse) => Unit = (_) => ()
    def disconnect : Future[_] = Future.failed(new NotImplementedError("disconnect"))
    def registerNotifyListener(listener: (NotificationResponse) => Unit) : Unit = this.listener = listener
    def sendQuery(query: String) : Future[QueryResult] = Future.failed(new NotImplementedError("sendQuery"))
    def connect : Future[_] = Future.failed(new NotImplementedError("connect"))
  }

  "ChangefeedListener" should {
    "establish connection on start" in {
      var connected = false
      val connection = connectionBuilder(new TestPostgresConnection {
        override def connect = {
          connected = true
          Future.successful(this)
        }
      })

      system.actorOf(ChangefeedListener.props(connection))

      connected mustEqual true
    }
    "register listener on start" in {
      var registered = false
      val connection = connectionBuilder(new TestPostgresConnection {
        override def connect = Future.successful(this)
        override def registerNotifyListener(listener: (NotificationResponse) => Unit): Unit = {
          registered = true
          super.registerNotifyListener(listener)
        }
      })

      system.actorOf(ChangefeedListener.props(connection))

      registered mustEqual true
    }
    "fail to start if connection can't be opened" in {
      val connection = connectionBuilder(new TestPostgresConnection())

      val deathWatch = TestProbe()

      val ref = system.actorOf(ChangefeedListener.props(connection))
      deathWatch.watch(ref)

      deathWatch.expectTerminated(ref)
    }

    "send empty state to parent on start" in {
      val connection = connectionBuilder(new TestPostgresConnection {
        override def connect = Future.successful(this)
      })
      val parent = TestProbe()

      parent.childActorOf(ChangefeedListener.props(connection))

      parent.expectMsg(ChannelsChanged(Set()))

    }
    "shutdown connection on stop" in {
      var disconnected = false
      val connection = connectionBuilder(new TestPostgresConnection {
        override def connect = Future.successful(this)
        override def disconnect: Future[_] = {
          disconnected = true
          Future.successful(this)
        }
      })

      val ref = system.actorOf(ChangefeedListener.props(connection))

      disconnected mustEqual false

      ref ! Kill

      disconnected mustEqual true
    }
    "throw exceptions when queries fail" in {
      var observedExc : Option[Throwable] = None
      val connection = connectionBuilder(new TestPostgresConnection {
        override def connect = Future.successful(this)
      })
      val parent = TestProbe()
      val strategy : SupervisorStrategy = new SupervisorStrategy {
        def handleChildTerminated(context: ActorContext, child: ActorRef, children: Iterable[ActorRef]) = ()
        def processFailure(context: ActorContext, restart: Boolean, child: ActorRef, cause: Throwable, stats: ChildRestartStats, children: Iterable[ChildRestartStats]) = ()
        def decider = {
          case exc =>
            observedExc = Some(exc)
            SupervisorStrategy.Restart
        }
      }
      val ref = parent.childActorOf(ChangefeedListener.props(connection), strategy)

      // should fail because sendQuery returns
      ref ! ListenChannels(Set("change_history"))

      observedExc.isDefined mustEqual true
    }
    "only allow one inflight query" in {
      var lastQueryPromise : Promise[QueryResult] = null
      var lastQuery : String = null
      var queryCount = 0
      val connection = connectionBuilder(new TestPostgresConnection {
        override def connect = Future.successful(this)
        override def sendQuery(query: String): Future[QueryResult] = {
          queryCount += 1
          lastQueryPromise = Promise()
          lastQuery = query
          lastQueryPromise.future
        }
      })
      val parent = TestProbe()

      val ref = parent.childActorOf(ChangefeedListener.props(connection))

      parent.expectMsg(ChannelsChanged(Set()))

      // increment the set to ensure testing order
      ref ! ListenChannels(Set("change_history"))
      ref ! ListenChannels(Set("change_history", "changefeed_primary"))

      queryCount mustEqual 1
      lastQuery mustEqual "LISTEN \"change_history\""

      lastQueryPromise.success(new QueryResult(1, "OK", None))

      queryCount mustEqual 2
      lastQuery mustEqual "LISTEN \"changefeed_primary\""

    }
    "prefer to open new subscriptions over closing old ones" in {
      var lastQueryPromise : Promise[QueryResult] = null
      var lastQuery : String = null
      var queryCount = 0
      val connection = connectionBuilder(new TestPostgresConnection {
        override def connect = Future.successful(this)
        override def sendQuery(query: String): Future[QueryResult] = {
          queryCount += 1
          lastQueryPromise = Promise()
          lastQuery = query
          lastQueryPromise.future
        }
      })
      val parent = TestProbe()

      val ref = parent.childActorOf(ChangefeedListener.props(connection))
      parent.expectMsg(ChannelsChanged(Set()))

      ref ! ListenChannels(Set("change_history"))

      queryCount mustEqual 1
      lastQuery mustEqual "LISTEN \"change_history\""

      ref ! ListenChannels(Set("changefeed_primary"))

      queryCount mustEqual 1
      lastQuery mustEqual "LISTEN \"change_history\""

      parent.msgAvailable mustEqual false
      lastQueryPromise.success(new QueryResult(1, "OK", None))
      parent.expectMsg(ChannelsChanged(Set("change_history")))

      queryCount mustEqual 2
      lastQuery mustEqual "LISTEN \"changefeed_primary\""

      parent.msgAvailable mustEqual false
      lastQueryPromise.success(new QueryResult(1, "OK", None))
      parent.expectMsg(ChannelsChanged(Set("change_history", "changefeed_primary")))

      queryCount mustEqual 3
      lastQuery mustEqual "UNLISTEN \"change_history\""

      parent.msgAvailable mustEqual false
      lastQueryPromise.success(new QueryResult(1, "OK", None))
      parent.expectMsg(ChannelsChanged(Set("changefeed_primary")))
    }

    "escapes channel names" in {
      var lastQueryPromise : Promise[QueryResult] = null
      var lastQuery : String = null
      var queryCount = 0
      val connection = connectionBuilder(new TestPostgresConnection {
        override def connect = Future.successful(this)
        override def sendQuery(query: String): Future[QueryResult] = {
          queryCount += 1
          lastQueryPromise = Promise()
          lastQuery = query
          lastQueryPromise.future
        }
      })
      val parent = TestProbe()

      val ref = parent.childActorOf(ChangefeedListener.props(connection))
      parent.expectMsg(ChannelsChanged(Set()))

      ref ! ListenChannels(Set("""change_history"; DROP TABLE resource;"""))

      queryCount mustEqual 1
      lastQuery mustEqual "LISTEN \"change_history\"\"; DROP TABLE resource;\""
    }
  }
}
