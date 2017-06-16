package testutil

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.{Args, Status, Suite, SuiteMixin}

trait SuiteActorSystem extends SuiteMixin { this: Suite =>

  implicit lazy val sys: ActorSystem = ActorSystem()
  implicit lazy val mat: ActorMaterializer = ActorMaterializer()

  abstract override def run(testName: Option[String], args: Args): Status = {
    try {
      val status = super.run(testName, args)
      status.whenCompleted { _ => sys.terminate() }
      status
    }
    catch { // In case the suite aborts, ensure the app is stopped
      case ex: Throwable =>
        sys.terminate()
        throw ex
    }
  }
}
