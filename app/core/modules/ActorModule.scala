package core.modules

import actors.{AckSubscriptionManager, ScratchExpanderManager}
import akka.actor.ActorSystem
import com.google.inject.{AbstractModule, Inject}
import play.api.{Environment, Logger, Mode}
import play.api.libs.concurrent.AkkaGuiceSupport

import scala.concurrent.ExecutionContext

sealed trait ActorSystemDeathPact

class ActualActorSystemDeathPact @Inject() (env: Environment, actorSystem: ActorSystem)(implicit executor: ExecutionContext) extends ActorSystemDeathPact {
  if (env.mode == Mode.Prod) {
    actorSystem.whenTerminated.andThen {
      case e =>
        Logger.warn(s"ActorSystem shutdown: $e")
        System.exit(1)
    }
  }
}

class ActorModule extends AbstractModule with AkkaGuiceSupport {
  def configure = {
    bind(classOf[ActorSystemDeathPact]).to(classOf[ActualActorSystemDeathPact]).asEagerSingleton()
    bindActor[AckSubscriptionManager]("subscription-manager")
    bindActor[ScratchExpanderManager]("expander-manager")
  }
}
