package to.holepunch.bare.stow.host

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject

/**
 * The framing of the control and user data channels, wire-compatible with the JavaScript
 * `bare-stow-protocol`.
 */
public object Frame {
  /** The tag of a control frame. */
  public const val CONTROL: Byte = 0x00

  /** The tag of a user data frame. */
  public const val USER: Byte = 0x01

  /** Encodes a frame of [tag] carrying [payload]. */
  public fun encode(tag: Byte, payload: ByteArray): ByteArray {
    val length = payload.size + 1

    return byteArrayOf(
      length.toByte(),
      (length shr 8).toByte(),
      (length shr 16).toByte(),
      (length shr 24).toByte(),
      tag,
    ) + payload
  }

  /** Encodes a control frame of [type] with integer [fields], in order. */
  public fun control(type: String, fields: Map<String, Int> = emptyMap()): ByteArray {
    val message = buildJsonObject {
      put("type", JsonPrimitive(type))

      for ((key, value) in fields) put(key, JsonPrimitive(value))
    }

    return encode(CONTROL, message.toString().encodeToByteArray())
  }
}

/** Decodes frames from a stream of bytes. */
public class FrameDecoder {
  private var buffer = ByteArray(0)

  /** Feeds [data] to the decoder and returns the frames it completes, as their tag and payload. */
  public fun feed(data: ByteArray): List<Pair<Byte, ByteArray>> {
    buffer += data

    val frames = mutableListOf<Pair<Byte, ByteArray>>()
    var offset = 0

    while (buffer.size - offset >= 4) {
      val length = (0 until 4).sumOf { (buffer[offset + it].toInt() and 0xff) shl (8 * it) }

      val start = offset + 4
      val end = start + length

      if (end > buffer.size) break

      if (length > 0) frames += buffer[start] to buffer.copyOfRange(start + 1, end)

      offset = end
    }

    buffer = buffer.copyOfRange(offset, buffer.size)

    return frames
  }
}

/** Decodes the JSON object of a control frame, or returns `null` if it isn't one. */
public fun decodeControl(payload: ByteArray): JsonObject? =
  try {
    Json.parseToJsonElement(payload.decodeToString()) as? JsonObject
  } catch (error: IllegalArgumentException) {
    null
  }
