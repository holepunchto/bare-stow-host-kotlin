package to.holepunch.bare.stow.host

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull

/** A byte transport to a stowed bundle, such as the IPC of a worklet from `bare-kit-kotlin`. */
public interface Transport {
  /** The data written by the bundle, until the transport ends. */
  public val data: Flow<ByteArray>

  /** Writes [data] to the bundle. */
  public suspend fun write(data: ByteArray)
}

/** A fault reported by or observed in the worker. */
public class WorkerError(
  message: String,
  /** The stack of the error reported by the worker, if any. */
  public val stack: String? = null,
  /** The exit code of a worker that exited without saying why. */
  public val exitCode: Int? = null,
  /** The signal that killed a worker that exited without saying why. */
  public val signal: Int? = null,
) : Exception(message)

/**
 * The host side of a stowed bundle, layering readiness, termination, and exit reporting over a
 * transport.
 *
 * [data] yields the user data written by the worker until the transport ends, and throws a
 * [WorkerError] once the worker has reported a fault or exited without saying why.
 *
 * @param transport The transport to the bundle.
 * @param kill Stops a worker that ignores `terminate` for [grace].
 * @param timeout How long to wait for the worker to signal ready before failing [ready] and
 *   terminating it, or `null` to wait indefinitely.
 * @param grace How long a terminating worker gets to stop and exit before [kill] is called.
 */
public class IPC(
  transport: Transport,
  private val kill: () -> Unit,
  timeout: Duration? = null,
  private val grace: Duration = DEFAULT_GRACE,
) {
  private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
  private val received = Channel<ByteArray>(Channel.UNLIMITED)
  private val writes = Channel<Pair<ByteArray, CompletableDeferred<Unit>?>>(Channel.UNLIMITED)
  private val readied = CompletableDeferred<Unit>()

  private val lock = Any()
  private var closed = false
  private var reportedExitCode: Int? = null
  private var exitReported = false
  private var failure: WorkerError? = null
  private var terminating: Deferred<Unit>? = null
  private var timer: Job? = null

  private val reading: Job

  /** The user data written by the worker. It can be collected only once. */
  public val data: Flow<ByteArray> = received.consumeAsFlow()

  init {
    scope.launch {
      for ((frame, written) in writes) {
        try {
          transport.write(frame)

          written?.complete(Unit)
        } catch (error: Exception) {
          written?.completeExceptionally(error)
        }
      }
    }

    writes.trySend(
      Frame.control("configure", mapOf("grace" to grace.inWholeMilliseconds.toInt())) to null
    )

    reading = scope.launch { readFrames(transport) }

    if (timeout != null) {
      val timer = scope.launch {
        delay(timeout)

        settleReady(WorkerError("Worker timed out before ready"))
      }

      synchronized(lock) { this.timer = timer }
    }
  }

  /** The exit code the worker reported, if any. */
  public val exitCode: Int?
    get() = synchronized(lock) { reportedExitCode }

  /**
   * Waits for the worker to signal ready.
   *
   * @throws WorkerError If the worker errors, exits, closes, or times out before then, in which
   *   case the worker is terminated.
   */
  public suspend fun ready(): Unit = readied.await()

  /** Writes [data] to the worker as user data. */
  public suspend fun write(data: ByteArray) {
    val written = CompletableDeferred<Unit>()

    if (writes.trySend(Frame.encode(Frame.USER, data) to written).isFailure) {
      throw WorkerError("Worker closed")
    }

    written.await()
  }

  /** Waits for the transport to end and returns the exit code the worker reported, if any. */
  public suspend fun wait(): Int? {
    reading.join()

    return exitCode
  }

  /**
   * Asks the worker to terminate and waits for the transport to end, calling `kill` if the worker
   * hasn't exited within the grace period.
   *
   * @return The exit code the worker reported, if any.
   */
  public suspend fun terminate(): Int? {
    close()

    return exitCode
  }

  /** Like [terminate], without the exit code. */
  public suspend fun close() {
    val terminating =
      synchronized(lock) { terminating ?: scope.async { runTerminate() }.also { terminating = it } }

    terminating.await()
  }

  /**
   * Reports that the worker has exited, as observed by the transport, such as the exit of a
   * worklet.
   *
   * If the worker exited without saying why and was not asked to, the handle fails with a
   * [WorkerError] carrying [code] and [signal].
   */
  public fun exited(code: Int?, signal: Int? = null) {
    val ignored = synchronized(lock) { exitReported || terminating != null || failure != null }

    if (ignored) return

    if (signal != null) {
      fail(WorkerError("Worker was killed by signal $signal", signal = signal))
    } else {
      fail(WorkerError("Worker exited with code ${code ?: -1}", exitCode = code))
    }
  }

  private suspend fun readFrames(transport: Transport) {
    val decoder = FrameDecoder()

    try {
      transport.data.collect { chunk ->
        for ((tag, payload) in decoder.feed(chunk)) onFrame(tag, payload)
      }
    } catch (error: CancellationException) {
      throw error
    } catch (error: Exception) {
      fail(WorkerError(error.message ?: error.toString()))
    }

    onClose()
  }

  private fun onFrame(tag: Byte, payload: ByteArray) {
    if (tag == Frame.USER) {
      received.trySend(payload)
      return
    }

    val message = (if (tag == Frame.CONTROL) decodeControl(payload) else null) ?: return

    when (message.string("type")) {
      "ready" -> settleReady(null)
      "exit" -> {
        val code = (message["code"] as? JsonPrimitive)?.intOrNull

        val faulted =
          synchronized(lock) {
            exitReported = true
            reportedExitCode = code
            failure != null
          }

        // An error frame precedes the exit frame of a worker that faulted.
        if (!faulted) settleReady(WorkerError("Worker exited with code ${code ?: -1} before ready"))
      }
      "error" -> fail(WorkerError(message.string("message") ?: "", stack = message.string("stack")))
    }
  }

  private fun onClose() {
    synchronized(lock) { closed = true }

    settleReady(WorkerError("Worker closed before ready"))

    received.close()
    writes.close()
  }

  private fun fail(error: WorkerError) {
    val first =
      synchronized(lock) {
        if (failure != null) return@synchronized false
        failure = error
        true
      }

    if (!first) return

    settleReady(error)

    received.close(error)

    scope.launch { close() }
  }

  // Settles ready with success when `error` is `null`, unless already settled.
  private fun settleReady(error: WorkerError?) {
    val settled =
      if (error == null) readied.complete(Unit) else readied.completeExceptionally(error)

    if (!settled) return

    synchronized(lock) { timer }?.cancel()

    if (error != null) scope.launch { close() }
  }

  private suspend fun runTerminate() {
    if (synchronized(lock) { closed }) return

    val killing = scope.launch {
      delay(grace)

      kill()
    }

    writes.trySend(Frame.control("terminate") to null)

    reading.join()

    killing.cancel()
  }

  private fun JsonObject.string(key: String): String? = (this[key] as? JsonPrimitive)?.contentOrNull

  /** Defaults of an [IPC]. */
  public companion object {
    /** The default time a terminating worker gets to stop and exit. */
    public val DEFAULT_GRACE: Duration = 5.seconds
  }
}
