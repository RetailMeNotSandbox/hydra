package core.stream


import akka.{DelayFix, NotUsed}
import akka.stream.{Attributes, DelayOverflowStrategy, FlowShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge}

import scala.collection.{immutable, mutable}
import scala.concurrent.duration.FiniteDuration


sealed trait WindowEvent[T]

case class WindowStart[T](evt: T) extends WindowEvent[T]

case class WindowEnd[T](evt: T) extends WindowEvent[T]

object Ops {
  // Emits the last element seen for a given key after `windowSize` has elapsed without seeing another element with the same key
  def dedupeWindow[T, U](windowSize: FiniteDuration, buffer: Int, overflowStrategy: DelayOverflowStrategy, key: (T) => U, eager: (T) => Boolean): Flow[T, T, NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit b: GraphDSL.Builder[NotUsed] =>
    import GraphDSL.Implicits._

    val broadcast = b.add(Broadcast[T](2))
    val merge = b.add(Merge[WindowEvent[T]](2))

    broadcast.out(0)
      .map(WindowStart[T]) ~> merge.in(0)

    val delayedFlow = Flow[T].via(new DelayFix(windowSize, overflowStrategy)).withAttributes(Attributes.inputBuffer(buffer, buffer))
    //    val delayedFlow = Flow[T].delay(windowSize, overflowStrategy).withAttributes(Attributes.inputBuffer(buffer, buffer))

    broadcast.out(1)
      .filterNot(eager)
      .via(delayedFlow)
      .map(WindowEnd[T]) ~> merge.in(1)

    val deduped = merge.out.statefulMapConcat[T] { () =>
      val window = mutable.Map[U, Int]()

      {
        case WindowStart(item) =>
          if (eager(item)) {
            immutable.Seq(item)
          } else {
            val itemKey = key(item)
            window.put(itemKey, window.getOrElse(itemKey, 0) + 1)
            immutable.Seq.empty
          }

        case WindowEnd(item) =>
          val itemKey = key(item)
          window.put(itemKey, window(itemKey) - 1)
          if (window(itemKey) == 0) {
            window.remove(itemKey)
            immutable.Seq(item)
          } else {
            immutable.Seq.empty
          }
      }
    }

    FlowShape(broadcast.in, deduped.outlet)
  })
}
