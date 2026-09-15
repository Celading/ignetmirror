package ignetmirror.Features.mirror

import ignetmirror.Commons.*
import std.collection.*
import std.fs.*
import std.sync.*
import std.time.*

/// Mirror sink: directional tee of everything that traversed the relay.
/// File sink writes one append-only file per run with framed records:
///   <epochMs> <DIR> <len> <ascii-or-escaped bytes>
/// plus a small header. Null sink discards (for pure netem runs).
public class MirrorSink {
    let mutex: Mutex = Mutex()
    let path: ?String
    let file: ?File

    /// Null sink.
    public init() {
        this.path = None
        this.file = None
    }

    /// File sink writing into `dir` (one combined stream file).
    public init(dir!: String) {
        if (!fsDirExists(dir)) {
            Directory.create(dir, recursive: true)
        }
        let p = "${dir}/mirror-stream.log"
        this.path = Some(p)
        this.file = Some(File(p, OpenMode.Append))
    }

    public func record(epochMs: Int64, dirTag: String, payload: Array<Byte>): Unit {
        synchronized(mutex) {
            match (file) {
                case Some(f) =>
                    // frame the whole record, then one write — keeps the
                    // line atomic on disk and avoids per-byte syscalls
                    let line = ArrayList<Byte>()
                    appendStr(line, epochMs.toString())
                    appendStr(line, " ")
                    appendStr(line, dirTag)
                    appendStr(line, " ")
                    appendStr(line, payload.size.toString())
                    appendStr(line, " ")
                    for (b in payload) {
                        if (b >= UInt8(32) && b < UInt8(127)) {
                            line.add(b)
                        } else {
                            appendStr(line, "\\x")
                            let hex = "0123456789ABCDEF"
                            line.add(hex[Int64((b >> 4) & 0x0F)])
                            line.add(hex[Int64(b & 0x0F)])
                        }
                    }
                    appendStr(line, "\r\n")
                    f.write(line.toArray())
                case None => ()
            }
        }
    }

    public func close(): Unit {
        synchronized(mutex) {
            match (file) {
                case Some(f) => try { f.close() } catch (_: Exception) { () }
                case None => ()
            }
        }
    }
}

func appendStr(sink: ArrayList<Byte>, s: String): Unit {
    for (b in s.toArray()) {
        sink.add(b)
    }
}

func fsDirExists(path: String): Bool {
    try {
        let info = FileInfo(path)
        info.isDirectory()
    } catch (_: Exception) {
        false
    }
}
