package testutil

import scala.concurrent.ExecutionContext

/**
  * [[ExecutionContext]] that will execute actions in calling thread (and by that making them blocking).
  *
  * https://gist.github.com/kermitas/41c456c839645ab300d3
  */
class CallingThreadExecutionContext extends ExecutionContext {

  override def execute(runnable: Runnable): Unit = runnable.run()

  override def reportFailure(t: Throwable): Unit = throw t
}
