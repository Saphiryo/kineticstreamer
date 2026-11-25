package com.kevmo314.kineticstreamer.kinetic

import java.io.Closeable

/**
 * Local RTSP server (rtsp://<device-ip>:<port>/<any path>). When [user] or [pass] is
 * non-empty, clients must authenticate (Basic or Digest).
 */
class RTSPServerSink(mimeTypes: String, port: String, user: String, pass: String) : Closeable {
    private var nativeHandle: Long

    init {
        // Ensure Kinetic library is loaded
        Kinetic

        nativeHandle = create(mimeTypes, port, user, pass)
        if (nativeHandle == 0L) {
            throw RuntimeException("Failed to create RTSPServerSink (port $port in use or unsupported codec?)")
        }
    }

    private external fun create(mimeTypes: String, port: String, user: String, pass: String): Long
    private external fun writeSample(handle: Long, streamIndex: Int, data: ByteArray, pts: Long, flags: Int)
    private external fun close(handle: Long)

    /**
     * @param streamIndex 0 for video, 1 for audio
     * @param flags MediaCodec buffer flags (codec config / keyframe)
     */
    fun writeSample(streamIndex: Int, data: ByteArray, ptsMicroseconds: Long, flags: Int) {
        require(streamIndex == 0 || streamIndex == 1) { "Invalid stream index: $streamIndex" }
        writeSample(nativeHandle, streamIndex, data, ptsMicroseconds, flags)
    }

    override fun close() {
        if (nativeHandle != 0L) {
            close(nativeHandle)
            nativeHandle = 0L
        }
    }
}
