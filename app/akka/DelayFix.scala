package akka

import java.util.concurrent.TimeUnit.NANOSECONDS

import akka.stream.Attributes.InputBuffer
import akka.stream.OverflowStrategies._
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.{Attributes, BufferOverflowException, DelayOverflowStrategy}
import akka.stream.impl.fusing.GraphStages.SimpleLinearGraphStage
import akka.stream.impl.{Buffer => BufferImpl}
import akka.stream.stage.{GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogic}

import scala.concurrent.duration._
import scala.util.Try

// temp workaround until https://github.com/akka/akka/issues/22416 gets fixed
final class DelayFix[T](val d: FiniteDuration, val strategy: DelayOverflowStrategy) extends SimpleLinearGraphStage[T] {
  private[this] def timerName = "DelayedTimer"

  override def initialAttributes: Attributes = DefaultAttributes.delay

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) with InHandler with OutHandler {
    val size =
      inheritedAttributes.get[InputBuffer] match {
        case None                        ⇒ throw new IllegalStateException(s"Couldn't find InputBuffer Attribute for $this")
        case Some(InputBuffer(min, max)) ⇒ max
      }
    val delayMillis = d.toMillis

    var buffer: BufferImpl[(Long, T)] = _ // buffer has pairs timestamp with upstream element

    override def preStart(): Unit = buffer = BufferImpl(size, materializer)

    val onPushWhenBufferFull: () ⇒ Unit = strategy match {
      case EmitEarly ⇒
        () ⇒ {
          if (!isTimerActive(timerName))
            push(out, buffer.dequeue()._2)
          else {
            cancelTimer(timerName)
            onTimer(timerName)
          }
        }
      case DropHead ⇒
        () ⇒ {
          buffer.dropHead()
          grabAndPull()
        }
      case DropTail ⇒
        () ⇒ {
          buffer.dropTail()
          grabAndPull()
        }
      case DropNew ⇒
        () ⇒ {
          grab(in)
          if (!isTimerActive(timerName)) scheduleOnce(timerName, d)
        }
      case DropBuffer ⇒
        () ⇒ {
          buffer.clear()
          grabAndPull()
        }
      case Fail ⇒
        () ⇒ {
          failStage(new BufferOverflowException(s"Buffer overflow for delay combinator (max capacity was: $size)!"))
        }
      case Backpressure ⇒
        () ⇒ {
          throw new IllegalStateException("Delay buffer must never overflow in Backpressure mode")
        }
    }

    def onPush(): Unit = {
      if (buffer.isFull)
        onPushWhenBufferFull()
      else {
        grabAndPull()
        if (!isTimerActive(timerName)) {
          scheduleOnce(timerName, d)
        }
      }
    }

    def pullCondition: Boolean =
      strategy != Backpressure || buffer.used < size

    def grabAndPull(): Unit = {
      buffer.enqueue((System.nanoTime(), grab(in)))
      if (pullCondition) pull(in)
    }

    override def onUpstreamFinish(): Unit =
      completeIfReady()

    def onPull(): Unit = {
      if (!isTimerActive(timerName) && !buffer.isEmpty) {
        val waitTime = nextElementWaitTime()
        if (waitTime < 0) {
          push(out, buffer.dequeue()._2)
        } else {
          scheduleOnce(timerName, Math.max(10, waitTime).millis)
        }
      }

      if (!isClosed(in) && !hasBeenPulled(in) && pullCondition)
        pull(in)

      completeIfReady()
    }

    setHandler(in, this)
    setHandler(out, this)

    def completeIfReady(): Unit = if (isClosed(in) && buffer.isEmpty) completeStage()

    def nextElementWaitTime(): Long = {
      delayMillis - NANOSECONDS.toMillis(System.nanoTime() - buffer.peek()._1)
    }

    final override protected def onTimer(key: Any): Unit = {
      if (isAvailable(out))
        push(out, buffer.dequeue()._2)

      if (!buffer.isEmpty) {
        val waitTime = nextElementWaitTime()
        if (waitTime > 10)
          scheduleOnce(timerName, waitTime.millis)
      }
      completeIfReady()
    }
  }

  override def toString = "DelayFix"
}
