package to.holepunch.bare.stow.host

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlinx.serialization.json.JsonPrimitive

class FrameTest {
  // Written by the JavaScript bare-stow-protocol for the same frames.
  @Test
  fun encodesFramesLikeTheJavaScriptImplementation() {
    assertEquals(
      "22000000007b2274797065223a22636f6e666967757265222c226772616365223a353030307d",
      hex(Frame.control("configure", mapOf("grace" to 5000))),
    )
    assertEquals("11000000007b2274797065223a227265616479227d", hex(Frame.control("ready")))
    assertEquals("060000000168656c6c6f", hex(Frame.encode(Frame.USER, "hello".encodeToByteArray())))
  }

  @Test
  fun decodesFramesSplitAcrossChunks() {
    val decoder = FrameDecoder()
    val data = Frame.encode(Frame.USER, "hello".encodeToByteArray()) + Frame.control("ready")

    val frames = data.flatMap { decoder.feed(byteArrayOf(it)) }

    assertEquals(listOf(Frame.USER, Frame.CONTROL), frames.map { it.first })
    assertEquals(listOf("hello", """{"type":"ready"}"""), frames.map { it.second.decodeToString() })
  }

  @Test
  fun decodesSeveralFramesFromOneChunk() {
    val decoder = FrameDecoder()
    val data =
      Frame.encode(Frame.USER, "a".encodeToByteArray()) +
        Frame.encode(Frame.USER, ByteArray(0)) +
        Frame.encode(Frame.USER, "b".encodeToByteArray())

    assertEquals(listOf("a", "", "b"), decoder.feed(data).map { it.second.decodeToString() })
  }

  @Test
  fun skipsEmptyFrames() {
    val decoder = FrameDecoder()
    val data = byteArrayOf(0, 0, 0, 0) + Frame.encode(Frame.USER, "a".encodeToByteArray())

    val frames = decoder.feed(data)

    assertEquals(1, frames.size)
    assertContentEquals("a".encodeToByteArray(), frames[0].second)
  }

  @Test
  fun ignoresInvalidControlPayloads() {
    assertNull(decodeControl("not json".encodeToByteArray()))
    assertNull(decodeControl("[1, 2]".encodeToByteArray()))
    assertEquals(
      JsonPrimitive("ready"),
      decodeControl("""{"type":"ready"}""".encodeToByteArray())?.get("type"),
    )
  }

  private fun hex(data: ByteArray) = data.joinToString("") { "%02x".format(it) }
}
