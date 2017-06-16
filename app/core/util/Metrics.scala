package core.util

import com.codahale.metrics.JvmAttributeGaugeSet
import com.codahale.metrics.jvm.{GarbageCollectorMetricSet, ThreadStatesGaugeSet, MemoryUsageGaugeSet}

import com.codahale.metrics.MetricRegistry
import nl.grons.metrics.scala.{Timer, InstrumentedBuilder, FutureMetrics}

import scala.concurrent.{ExecutionContext, Future}

object Metrics {
  /** The application wide metrics registry. */
  val metricRegistry = new MetricRegistry()
  metricRegistry.register("jvm.attribute", new JvmAttributeGaugeSet())
  metricRegistry.register("jvm.gc", new GarbageCollectorMetricSet())
  metricRegistry.register("jvm.memory", new MemoryUsageGaugeSet())
  metricRegistry.register("jvm.threads", new ThreadStatesGaugeSet())
}

class CurriedTiming(timer: Timer)(implicit context: ExecutionContext) {
  def apply[A](builder: => Future[A]) : Future[A] = {
    val ctx = timer.timerContext
    val f = builder
    f.onComplete(_ => ctx.stop())
    f
  }
}
trait Instrumented extends InstrumentedBuilder {
  val metricRegistry = Metrics.metricRegistry

  def timing(timerName: String)(implicit context: ExecutionContext) = new CurriedTiming(metrics.timer(timerName))
  def timing(timerName: String, scope: String)(implicit context: ExecutionContext) = new CurriedTiming(metrics.timer(timerName, scope))
}
