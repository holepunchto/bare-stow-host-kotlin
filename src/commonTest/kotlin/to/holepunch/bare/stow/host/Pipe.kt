package to.holepunch.bare.stow.host

import java.io.IOException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull

class End {
  private val received = Channel<ByteArray>(Channel.UNLIMITED)

  lateinit var peer: End

  @Volatile
  var closed = false
    private set

  val data: Flow<ByteArray> = received.consumeAsFlow()

  suspend fun write(data: ByteArray) {
    if (closed) throw IOException("Broken pipe")

    peer.received.trySend(data)
  }

  fun close() {
    for (end in listOf(this, peer)) {
      end.closed = true
      end.received.close()
    }
  }
}

fun pair(): Pair<End, End> {
  val a = End()
  val b = End()

  a.peer = b
  b.peer = a

  return a to b
}

class Worker(private val end: End) {
  private val frames = Channel<Pair<Byte, ByteArray>>(Channel.UNLIMITED)

  init {
    CoroutineScope(Dispatchers.Default).launch {
      val decoder = FrameDecoder()

      end.data.collect { chunk -> for (frame in decoder.feed(chunk)) frames.trySend(frame) }

      frames.close()
    }
  }

  suspend fun send(type: String, fields: Map<String, Int> = emptyMap()) =
    end.write(Frame.control(type, fields))

  suspend fun sendError(message: String) =
    end.write(
      Frame.encode(Frame.CONTROL, """{"type":"error","message":"$message"}""".encodeToByteArray())
    )

  suspend fun write(data: ByteArray) = end.write(Frame.encode(Frame.USER, data))

  suspend fun receive(type: String): JsonObject? {
    for ((tag, payload) in frames) {
      val message = if (tag == Frame.CONTROL) decodeControl(payload) else null

      if ((message?.get("type") as? JsonPrimitive)?.contentOrNull == type) return message
    }

    return null
  }

  suspend fun data(): ByteArray? {
    for ((tag, payload) in frames) if (tag == Frame.USER) return payload

    return null
  }

  fun close() = end.close()
}
