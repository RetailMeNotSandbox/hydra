import akka.actor.{ActorRef, ActorRefFactory}

package object actors {
  type ActorBuilder = ActorRefFactory => ActorRef
}
