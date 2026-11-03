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

/** A fault reported by or observed in the worker. */
public class WorkerError : Exception {
  /**
   * What went wrong: `WORKER_ERROR` for a fault the worker reported, `WORKER_EXITED` with
   * [exitCode] for a worker that exited, `WORKER_KILLED` with [signal] for a worker killed by a
   * signal, `WORKER_CLOSED` for a transport that closed before ready or a write after close,
   * `WORKER_TIMEOUT` for a worker that timed out before ready, and `TRANSPORT_ERROR` with [cause]
   * for a transport that failed while reading or writing. A worker that exits within the grace
   * period of a transport failure fails with its exit instead.
   */
  public val code: String

  /** The stack of the error reported by the worker, if any. */
  public val stack: String?

  /** The exit code of a worker that exited, if known. */
  public val exitCode: Int?

  /** The signal that killed a worker that exited without saying why. */
  public val signal: Int?

  /** Creates a `WORKER_ERROR` error. */
  public constructor(
    message: String,
    stack: String? = null,
    exitCode: Int? = null,
    signal: Int? = null,
  ) : this(message, stack, exitCode, signal, "WORKER_ERROR")

  /** Creates an error of [code], caused by [cause] if any. */
  public constructor(
    message: String,
    stack: String? = null,
    exitCode: Int? = null,
    signal: Int? = null,
    code: String = "WORKER_ERROR",
    cause: Throwable? = null,
  ) : super(message, cause) {
    this.code = code
    this.stack = stack
    this.exitCode = exitCode
    this.signal = signal
  }
}

/**
 * The host side of a stowed bundle, layering readiness, termination, and exit reporting over a byte
 * transport to it, such as the IPC of a worklet from `bare-kit-kotlin`.
 *
 * @param data The data written by the bundle, until the transport ends.
 * @param write Writes data to the bundle.
 * @param kill Stops a worker that ignores `terminate` for [grace].
 * @param timeout How long to wait for the worker to signal ready before failing [ready] and
 *   terminating it, or `null` to wait indefinitely.
 * @param grace How long a terminating worker gets to stop and exit before [kill] is called.
 *
 * The creator of the handle must report the exit of the worker with [exited]. The end of a
 * transport that ends without the worker saying why is held back until then, so that [data],
 * [ready], [wait], and [close] fail or return with the reason. A transport that fails instead waits
 * for the exit for [grace], failing with the exit and the failure as its cause, or calling [kill]
 * and failing with `TRANSPORT_ERROR` if the worker hasn't exited by then.
 */
public class Ipc(
  data: Flow<ByteArray>,
  write: suspend (ByteArray) -> Unit,
  private val kill: () -> Unit,
  timeout: Duration? = null,
  private val grace: Duration = DEFAULT_GRACE,
) {
  private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
  private val received = Channel<ByteArray>(Channel.UNLIMITED)
  private val writes = Channel<Pair<ByteArray, CompletableDeferred<Unit>?>>(Channel.UNLIMITED)
  private val readied = CompletableDeferred<Unit>()
  private val failed = CompletableDeferred<Unit>()

  private val lock = Any()
  private var closed = false
  private var reportedExitCode: Int? = null
  private var exitReported = false
  private var failure: WorkerError? = null
  private var terminating: Deferred<Unit>? = null
  private var terminateSent = false
  private var transportFailure: WorkerError? = null
  private var failing: Job? = null
  private var timer: Job? = null

  private val reading: Job

  /**
   * The user data written by the worker until the transport ends. It throws a [WorkerError] once
   * the worker has reported a fault or exited without saying why, and can be collected only once.
   */
  public val data: Flow<ByteArray> = received.consumeAsFlow()

  init {
    scope.launch {
      for ((frame, written) in writes) {
        try {
          write(frame)

          written?.complete(Unit)
        } catch (error: CancellationException) {
          written?.completeExceptionally(error)
        } catch (error: Exception) {
          written?.completeExceptionally(transportFailed(error))
        }
      }
    }

    writes.trySend(
      Frame.control("configure", mapOf("grace" to grace.inWholeMilliseconds.toInt())) to null
    )

    reading = scope.launch { readFrames(data) }

    if (timeout != null) {
      val timer = scope.launch {
        delay(timeout)

        settleReady(WorkerError("Worker timed out before ready", code = "WORKER_TIMEOUT"))
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
      throw WorkerError("Worker closed", code = "WORKER_CLOSED")
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
   * `WORKER_EXITED` or `WORKER_KILLED` [WorkerError] carrying [code] and [signal].
   */
  public fun exited(code: Int?, signal: Int? = null) {
    val cause =
      synchronized(lock) {
        if (exitReported || terminateSent || failure != null) return

        failing?.cancel()

        transportFailure
      }

    if (signal != null) {
      fail(
        WorkerError(
          "Worker was killed by signal $signal",
          code = "WORKER_KILLED",
          exitCode = code,
          signal = signal,
          cause = cause,
        )
      )
    } else {
      fail(
        WorkerError(
          "Worker exited with code ${code ?: -1}",
          code = "WORKER_EXITED",
          exitCode = code,
          cause = cause,
        )
      )
    }
  }

  private suspend fun readFrames(data: Flow<ByteArray>) {
    val decoder = FrameDecoder()

    try {
      data.collect { chunk -> for ((tag, payload) in decoder.feed(chunk)) onFrame(tag, payload) }
    } catch (error: CancellationException) {
      throw error
    } catch (error: Exception) {
      transportFailed(error)
    }

    // A worker whose transport ends without it saying why has crashed, so the end is held back
    // until its exit is reported for readers to fail with. It can no longer time out, as it
    // already ended.
    synchronized(lock) { closed = true }

    if (crashed()) {
      synchronized(lock) { timer }?.cancel()

      failed.await()
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
        if (!faulted) {
          settleReady(
            WorkerError(
              "Worker exited with code ${code ?: -1} before ready",
              code = "WORKER_EXITED",
              exitCode = code,
            )
          )
        }
      }
      "error" ->
        fail(
          WorkerError(
            message.string("message") ?: "",
            code = "WORKER_ERROR",
            stack = message.string("stack"),
          )
        )
    }
  }

  private fun crashed(): Boolean =
    synchronized(lock) { !exitReported && !terminateSent && failure == null }

  private fun onClose() {
    settleReady(WorkerError("Worker closed before ready", code = "WORKER_CLOSED"))

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

    failed.complete(Unit)

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
    val open =
      synchronized(lock) {
        if (!closed) terminateSent = true

        !closed
      }

    if (!open) return reading.join()

    val killing = scope.launch {
      delay(grace)

      kill()
    }

    writes.trySend(Frame.control("terminate") to null)

    reading.join()

    killing.cancel()
  }

  // Returns the error to give the writer whose write failed. A transport that fails while the
  // worker may have crashed waits for its exit as an end does, killing it if it hasn't exited
  // within the grace period.
  private fun transportFailed(error: Exception): WorkerError {
    val failure =
      WorkerError(error.message ?: error.toString(), code = "TRANSPORT_ERROR", cause = error)

    synchronized(lock) {
      if (closed || exitReported || terminateSent) {
        return WorkerError("Worker closed", code = "WORKER_CLOSED")
      }

      if (this.failure != null || transportFailure != null) return failure

      transportFailure = failure

      timer?.cancel()

      failing = scope.launch {
        delay(grace)

        kill()

        fail(failure)
      }
    }

    return failure
  }

  private fun JsonObject.string(key: String): String? = (this[key] as? JsonPrimitive)?.contentOrNull

  /** Defaults of an [Ipc]. */
  public companion object {
    /** The default time a terminating worker gets to stop and exit. */
    public val DEFAULT_GRACE: Duration = 5.seconds
  }
}
