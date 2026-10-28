package to.holepunch.bare.stow.host

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.intOrNull

class IPCTest {
  private fun test(block: suspend kotlinx.coroutines.CoroutineScope.() -> Unit) = runBlocking {
    withTimeout(10.seconds) { block() }
  }

  private suspend fun assertReadyFails(ipc: IPC, message: String): WorkerError {
    val error = assertFailsWith<WorkerError> { ipc.ready() }

    assertEquals(message, error.message)

    return error
  }

  @Test
  fun resolvesReadyWhenTheWorkerSignals() = test {
    val (left, right) = pair()
    val ipc = IPC(left, kill = { left.close() })
    val worker = Worker(right)

    worker.send("ready")
    ipc.ready()

    worker.close()
    ipc.wait()
  }

  @Test
  fun sendsTheGraceToTheWorker() = test {
    val (left, right) = pair()
    val ipc = IPC(left, kill = { left.close() }, grace = 2500.milliseconds)
    val worker = Worker(right)

    val message = worker.receive("configure")

    assertEquals(2500, (message?.get("grace") as? JsonPrimitive)?.intOrNull)

    worker.close()
    ipc.wait()
  }

  @Test
  fun failsReadyWhenTheWorkerErrorsBeforeReady() = test {
    val (left, right) = pair()
    val ipc = IPC(left, kill = { left.close() })
    val worker = Worker(right)

    worker.sendError("boom")

    assertReadyFails(ipc, "boom")

    worker.receive("terminate")
    worker.close()
  }

  @Test
  fun failsReadyWhenTheWorkerExitsBeforeReady() = test {
    val (left, right) = pair()
    val ipc = IPC(left, kill = { left.close() })
    val worker = Worker(right)

    worker.send("exit", mapOf("code" to 1))

    assertReadyFails(ipc, "Worker exited with code 1 before ready")

    worker.receive("terminate")
    worker.close()
  }

  @Test
  fun failsReadyWhenTheTransportClosesBeforeReady() = test {
    val (left, _) = pair()
    val ipc = IPC(left, kill = { left.close() })

    left.close()

    assertReadyFails(ipc, "Worker closed before ready")
  }

  @Test
  fun ignoresExitAfterReady() = test {
    val (left, right) = pair()
    val ipc = IPC(left, kill = { left.close() })
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
    val ipc = IPC(left, kill = { left.close() })
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
    val ipc = IPC(left, kill = { left.close() })
    val worker = Worker(right)

    worker.write("payload".encodeToByteArray())

    val received = ipc.data.produceIn(this)

    val payload = received.receive()

    ipc.write("reply".encodeToByteArray())

    val replied = worker.data()

    worker.close()

    assertContentEquals("payload".encodeToByteArray(), payload)
    assertContentEquals("reply".encodeToByteArray(), replied)
    assertTrue(received.receiveCatching().isClosed)
  }

  @Test
  fun terminateReturnsTheExitCode() = test {
    val (left, right) = pair()
    val ipc = IPC(left, kill = { left.close() })
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
    val ipc = IPC(left, kill = { left.close() })
    val worker = Worker(right)

    val closing = async {
      worker.receive("terminate")
      worker.close()
    }

    ipc.close()
    closing.await()
  }

  @Test
  fun waitReturnsWhenTheTransportCloses() = test {
    val (left, _) = pair()
    val ipc = IPC(left, kill = { left.close() })

    left.close()

    assertNull(ipc.wait())
  }

  @Test
  fun failsReadyAndTerminatesTheWorkerOnTimeout() = test {
    val (left, right) = pair()
    val ipc = IPC(left, kill = { left.close() }, timeout = 10.milliseconds)
    val worker = Worker(right)

    assertReadyFails(ipc, "Worker timed out before ready")

    worker.receive("terminate")
    worker.close()
  }

  @Test
  fun terminateKillsTheWorkerAfterTheGracePeriod() = test {
    val (left, right) = pair()
    val ipc = IPC(left, kill = { left.close() }, grace = 10.milliseconds)
    val worker = Worker(right)

    worker.send("ready")
    ipc.ready()

    assertNull(ipc.terminate())
    assertTrue(left.closed)
  }

  @Test
  fun closeKillsTheWorkerAfterTheGracePeriod() = test {
    val (left, _) = pair()
    val ipc = IPC(left, kill = { left.close() }, grace = 10.milliseconds)

    ipc.close()

    assertTrue(left.closed)
  }

  @Test
  fun terminateReturnsOnceTheTransportHasClosed() = test {
    val (left, _) = pair()
    val ipc = IPC(left, kill = { left.close() })

    left.close()

    assertReadyFails(ipc, "Worker closed before ready")

    assertNull(ipc.terminate())
  }

  @Test
  fun exitedBeforeReadyFailsReadyWithTheExitCode() = test {
    val (left, _) = pair()
    val ipc = IPC(left, kill = { left.close() }, grace = 10.milliseconds)

    ipc.exited(3)

    val error = assertReadyFails(ipc, "Worker exited with code 3")

    assertEquals(3, error.exitCode)

    ipc.close()
  }

  @Test
  fun exitedAfterReadyFailsWithTheSignal() = test {
    val (left, right) = pair()
    val ipc = IPC(left, kill = { left.close() }, grace = 10.milliseconds)
    val worker = Worker(right)

    worker.send("ready")
    ipc.ready()

    ipc.exited(null, signal = 9)

    val error = assertFailsWith<WorkerError> { ipc.data.collect() }

    assertEquals("Worker was killed by signal 9", error.message)
    assertEquals(9, error.signal)

    ipc.close()
  }

  @Test
  fun exitedIsIgnoredAfterAnExitFrame() = test {
    val (left, right) = pair()
    val ipc = IPC(left, kill = { left.close() })
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
    val ipc = IPC(left, kill = { left.close() }, grace = 10.milliseconds)
    val worker = Worker(right)

    worker.send("ready")
    ipc.ready()

    val terminated = async { ipc.terminate() }

    worker.receive("terminate")

    ipc.exited(null, signal = 9)

    assertNull(terminated.await())

    ipc.data.collect()
  }
}
