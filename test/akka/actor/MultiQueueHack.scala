package akka.actor

import akka.dispatch.Envelope

object MultiQueueHack {
  // horrible hack to enqueue a message without triggering work
  // FOR TESTING PURPOSES ONLY

  // "Forgive me Testers, for I have sinned..."
  def enqueueDirectly(from: ActorRef, to: ActorRef, sys: ActorSystem, msgs: Any*) : Unit = {
    val mbox = to.asInstanceOf[LocalActorRef]
      .underlying
      .mailbox

    msgs.foreach(msg => mbox.enqueue(to, Envelope(msg, from, sys)))
  }
}
