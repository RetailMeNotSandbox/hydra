package testutil

import akka.actor.Actor

class ErrorActor extends Actor {
  def receive = {
    case ex: Exception => throw ex
  }
}
