package to.holepunch.bare.stow.host

import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.intOrNull

class IpcTest {
  private fun test(block: suspend kotlinx.coroutines.CoroutineScope.() -> Unit) = runBlocking {
    withTimeout(10.seconds) { block() }
  }

  private suspend fun assertReadyFails(ipc: Ipc, code: String): WorkerError {
    val error = assertFailsWith<WorkerError> { ipc.ready() }

    assertEquals(code, error.code)

    return error
  }

  @Test
  fun resolvesReadyWhenTheWorkerSignals() = test {
    val (left, right) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() })
    val worker = Worker(right)

    worker.send("ready")
    ipc.ready()

    worker.send("exit", mapOf("code" to 0))
    worker.close()
    ipc.wait()
  }

  @Test
  fun sendsTheGraceToTheWorker() = test {
    val (left, right) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() }, grace = 2500.milliseconds)
    val worker = Worker(right)

    val message = worker.receive("configure")

    assertEquals(2500, (message?.get("grace") as? JsonPrimitive)?.intOrNull)

    worker.send("exit", mapOf("code" to 0))
    worker.close()
    ipc.wait()
  }

  @Test
  fun failsReadyWhenTheWorkerErrorsBeforeReady() = test {
    val (left, right) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() })
    val worker = Worker(right)

    worker.sendError("boom")

    assertEquals("boom", assertReadyFails(ipc, "WORKER_ERROR").message)

    worker.receive("terminate")
    worker.close()
  }

  @Test
  fun failsReadyWhenTheWorkerExitsBeforeReady() = test {
    val (left, right) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() })
    val worker = Worker(right)

    worker.send("exit", mapOf("code" to 1))

    val error = assertReadyFails(ipc, "WORKER_EXITED")

    assertEquals(1, error.exitCode)

    worker.receive("terminate")
    worker.close()
  }

  @Test
  fun failsReadyWhenTheTransportClosesBeforeReadyWhileTerminating() = test {
    val (left, right) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() })
    val worker = Worker(right)

    val closing = async { ipc.close() }

    worker.receive("terminate")
    worker.close()

    assertReadyFails(ipc, "WORKER_CLOSED")

    closing.await()
  }

  @Test
  fun ignoresExitAfterReady() = test {
    val (left, right) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() })
    val worker = Worker(right)

    worker.send("ready")
    ipc.ready()

    worker.send("exit", mapOf("code" to 1))
    worker.close()
    ipc.wait()

    ipc.ready()
  }

  @Test
  fun reportsTheExitCode() = test {
    val (left, right) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() })
    val worker = Worker(right)

    worker.send("ready")
    worker.send("exit", mapOf("code" to 7))
    worker.close()

    assertEquals(7, ipc.wait())
    assertEquals(7, ipc.exitCode)
  }

  @Test
  fun carriesUserData() = test {
    val (left, right) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() })
    val worker = Worker(right)

    worker.write("payload".encodeToByteArray())

    val received = ipc.data.produceIn(this)

    val payload = received.receive()

    ipc.write("reply".encodeToByteArray())

    val replied = worker.data()

    worker.send("exit", mapOf("code" to 0))
    worker.close()

    assertContentEquals("payload".encodeToByteArray(), payload)
    assertContentEquals("reply".encodeToByteArray(), replied)

    val end = received.receiveCatching()

    assertTrue(end.isClosed)
    assertNull(end.exceptionOrNull())
  }

  @Test
  fun terminateReturnsTheExitCode() = test {
    val (left, right) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() })
    val worker = Worker(right)

    worker.send("ready")
    ipc.ready()

    val exiting = async {
      worker.receive("terminate")
      worker.send("exit", mapOf("code" to 0))
      worker.close()
    }

    val code = ipc.terminate()

    exiting.await()

    assertEquals(0, code)
  }

  @Test
  fun closeSendsTerminateToTheWorker() = test {
    val (left, right) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() })
    val worker = Worker(right)

    val closing = async {
      worker.receive("terminate")
      worker.close()
    }

    ipc.close()
    closing.await()
  }

  @Test
  fun waitReturnsOnceTheExitOfAClosedTransportIsReported() = test {
    val (left, _) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() })

    left.close()
    ipc.exited(0)

    assertNull(ipc.wait())

    val error = assertFailsWith<WorkerError> { ipc.write("late".encodeToByteArray()) }

    assertEquals("WORKER_CLOSED", error.code)
  }

  @Test
  fun failsReadyWhenTheTransportFails() = test {
    val failure = IOException("Input/output error")
    val ipc = Ipc(flow { throw failure }, {}, kill = {}, grace = 10.milliseconds)

    val error = assertReadyFails(ipc, "TRANSPORT_ERROR")

    assertEquals("Input/output error", error.message)
    assertSame(failure, error.cause)
  }

  @Test
  fun failsACrashedWorkerWithItsExitRatherThanATransportFailure() = test {
    val failure = IOException("Input/output error")
    val failed = CompletableDeferred<Unit>()
    val ipc =
      Ipc(
        flow {
          failed.complete(Unit)
          throw failure
        },
        {},
        kill = {},
      )

    failed.await()

    assertNull(withTimeoutOrNull(50.milliseconds) { ipc.wait() })

    ipc.exited(null, signal = 11)

    val error = assertReadyFails(ipc, "WORKER_KILLED")
    val cause = error.cause as WorkerError

    assertEquals("TRANSPORT_ERROR", cause.code)
    assertSame(failure, cause.cause)
  }

  @Test
  fun killsAWorkerWhoseTransportFailsAndThatDoesNotExit() = test {
    val failure = IOException("Input/output error")
    val killed = CompletableDeferred<Unit>()
    val ipc =
      Ipc(
        flow { throw failure },
        {},
        kill = { killed.complete(Unit) },
        grace = 10.milliseconds,
      )

    val error = assertReadyFails(ipc, "TRANSPORT_ERROR")

    assertEquals("Input/output error", error.message)
    assertSame(failure, error.cause)
    assertTrue(killed.isCompleted)
  }

  @Test
  fun treatsAResetTransportAsClosed() = test {
    val ended = CompletableDeferred<Unit>()
    val ipc =
      Ipc(
        flow {
          ended.complete(Unit)
          throw IOException("Connection reset by peer")
        },
        {},
        kill = {},
      )

    ended.await()

    assertNull(withTimeoutOrNull(50.milliseconds) { ipc.wait() })

    ipc.exited(1)

    val error = assertReadyFails(ipc, "WORKER_EXITED")

    assertNull(error.cause)
  }

  @Test
  fun failsReadyAndTerminatesTheWorkerOnTimeout() = test {
    val (left, right) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() }, timeout = 10.milliseconds)
    val worker = Worker(right)

    assertReadyFails(ipc, "WORKER_TIMEOUT")

    worker.receive("terminate")
    worker.close()
  }

  @Test
  fun terminateKillsTheWorkerAfterTheGracePeriod() = test {
    val (left, right) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() }, grace = 10.milliseconds)
    val worker = Worker(right)

    worker.send("ready")
    ipc.ready()

    assertNull(ipc.terminate())
    assertTrue(left.closed)
  }

  @Test
  fun closeKillsTheWorkerAfterTheGracePeriod() = test {
    val (left, _) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() }, grace = 10.milliseconds)

    ipc.close()

    assertTrue(left.closed)
  }

  @Test
  fun terminateReturnsOnceTheTransportHasClosed() = test {
    val (left, _) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() })

    left.close()
    ipc.exited(1)

    assertReadyFails(ipc, "WORKER_EXITED")

    assertNull(ipc.terminate())
  }

  @Test
  fun exitedBeforeReadyFailsReadyWithTheExitCode() = test {
    val (left, _) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() }, grace = 10.milliseconds)

    ipc.exited(3)

    val error = assertReadyFails(ipc, "WORKER_EXITED")

    assertEquals(3, error.exitCode)

    ipc.close()
  }

  @Test
  fun exitedWithACodeAndASignalCarriesBoth() = test {
    val (left, _) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() }, grace = 10.milliseconds)

    ipc.exited(137, signal = 9)

    val error = assertReadyFails(ipc, "WORKER_KILLED")

    assertEquals(137, error.exitCode)
    assertEquals(9, error.signal)

    ipc.close()
  }

  @Test
  fun failsAWriteWhenTheTransportFails() = test {
    val (left, _) = pair()
    val failure = IOException("Input/output error")
    val ipc = Ipc(left.data, { throw failure }, kill = { left.close() }, grace = 10.milliseconds)

    val error = assertFailsWith<WorkerError> { ipc.write("payload".encodeToByteArray()) }

    assertEquals("Input/output error", error.message)
    assertEquals("TRANSPORT_ERROR", error.code)
    assertSame(failure, error.cause)

    left.close()
    ipc.wait()
  }

  @Test
  fun failsAWriteToAWorkerThatWentAwayAsClosed() = test {
    val (left, _) = pair()
    val ipc = Ipc(left.data, { throw IOException("Broken pipe") }, kill = { left.close() })

    val error = assertFailsWith<WorkerError> { ipc.write("payload".encodeToByteArray()) }

    assertEquals("WORKER_CLOSED", error.code)

    left.close()
    ipc.exited(0)

    assertReadyFails(ipc, "WORKER_EXITED")
  }

  @Test
  fun ignoresATransportFailureAfterAnExitFrame() = test {
    val (left, right) = pair()
    var broken = false
    val ipc =
      Ipc(
        left.data,
        {
          if (broken) throw IOException("Input/output error")
          left.write(it)
        },
        kill = { left.close() },
      )
    val worker = Worker(right)
    val received = ipc.data.produceIn(this)

    worker.send("ready")
    worker.send("exit", mapOf("code" to 0))
    worker.write("payload".encodeToByteArray())

    received.receive()

    broken = true

    val error = assertFailsWith<WorkerError> { ipc.write("reply".encodeToByteArray()) }

    assertEquals("WORKER_CLOSED", error.code)

    worker.close()

    val end = received.receiveCatching()

    assertTrue(end.isClosed)
    assertNull(end.exceptionOrNull())
  }

  @Test
  fun exitedAfterReadyFailsWithTheSignal() = test {
    val (left, right) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() }, grace = 10.milliseconds)
    val worker = Worker(right)

    worker.send("ready")
    ipc.ready()

    ipc.exited(null, signal = 9)

    val error = assertFailsWith<WorkerError> { ipc.data.collect() }

    assertEquals("WORKER_KILLED", error.code)
    assertEquals(9, error.signal)

    ipc.close()
  }

  @Test
  fun exitedIsIgnoredAfterAnExitFrame() = test {
    val (left, right) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() })
    val worker = Worker(right)

    worker.send("ready")
    ipc.ready()

    worker.send("exit", mapOf("code" to 0))
    worker.close()
    ipc.wait()

    ipc.exited(0)

    ipc.data.collect()
  }

  @Test
  fun exitedIsIgnoredWhileTerminating() = test {
    val (left, right) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() }, grace = 10.milliseconds)
    val worker = Worker(right)

    worker.send("ready")
    ipc.ready()

    val terminated = async { ipc.terminate() }

    worker.receive("terminate")

    ipc.exited(null, signal = 9)

    assertNull(terminated.await())

    ipc.data.collect()
  }

  @Test
  fun holdsTheEndOfAWorkerThatCrashesAfterReady() = test {
    val (left, right) = pair()
    val ended = CompletableDeferred<Unit>()
    val ipc =
      Ipc(
        left.data.onCompletion { ended.complete(Unit) },
        left::write,
        kill = { left.close() },
      )
    val worker = Worker(right)

    worker.send("ready")
    ipc.ready()

    val reading = async { assertFailsWith<WorkerError> { ipc.data.collect() } }

    worker.close()
    ended.await()

    assertNull(withTimeoutOrNull(50.milliseconds) { ipc.wait() })

    ipc.exited(null, signal = 11)

    val error = reading.await()

    assertEquals("WORKER_KILLED", error.code)
    assertEquals(11, error.signal)

    ipc.wait()
  }

  @Test
  fun holdsTheEndOfAWorkerThatCrashesBeforeReady() = test {
    val (left, right) = pair()
    val ended = CompletableDeferred<Unit>()
    val ipc =
      Ipc(
        left.data.onCompletion { ended.complete(Unit) },
        left::write,
        kill = { left.close() },
      )
    val worker = Worker(right)

    worker.close()
    ended.await()

    assertNull(withTimeoutOrNull(50.milliseconds) { ipc.wait() })

    ipc.exited(1)

    val error = assertReadyFails(ipc, "WORKER_EXITED")

    assertEquals(1, error.exitCode)

    ipc.close()
  }

  @Test
  fun failsACrashedWorkerWithItsExitRatherThanATimeout() = test {
    val (left, right) = pair()
    val ipc =
      Ipc(
        left.data,
        left::write,
        kill = { left.close() },
        timeout = 50.milliseconds,
      )
    val worker = Worker(right)

    worker.close()

    delay(100.milliseconds)

    ipc.exited(1)

    val error = assertReadyFails(ipc, "WORKER_EXITED")

    assertEquals(1, error.exitCode)
  }

  @Test
  fun exitedAppliesToACloseAfterTheTransportEnded() = test {
    val (left, right) = pair()
    val ended = CompletableDeferred<Unit>()
    var killed = false
    val ipc =
      Ipc(
        left.data.onCompletion { ended.complete(Unit) },
        left::write,
        kill = { killed = true },
        grace = 10.milliseconds,
      )
    val worker = Worker(right)

    worker.send("ready")
    ipc.ready()

    worker.close()
    ended.await()

    assertNull(withTimeoutOrNull(50.milliseconds) { ipc.wait() })

    val closing = async(start = CoroutineStart.UNDISPATCHED) { ipc.close() }

    delay(50.milliseconds)

    ipc.exited(null, signal = 9)

    val error = assertFailsWith<WorkerError> { ipc.data.collect() }

    assertEquals("WORKER_KILLED", error.code)

    closing.await()

    assertFalse(killed)
  }

  @Test
  fun endsAWorkerThatExitsOnItsOwnWithoutHolding() = test {
    val (left, right) = pair()
    val ipc = Ipc(left.data, left::write, kill = { left.close() })
    val worker = Worker(right)

    worker.send("ready")
    ipc.ready()

    worker.send("exit", mapOf("code" to 0))
    worker.close()

    ipc.data.collect()

    assertEquals(0, ipc.wait())
  }
}
