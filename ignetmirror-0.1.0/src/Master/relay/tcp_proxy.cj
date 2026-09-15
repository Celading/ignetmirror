package ignetmirror.Master.relay

import ignetmirror.Commons.*
import ignetmirror.Features.mirror.*
import ignetmirror.Features.netem.*
import std.collection.*
import std.net.*
import std.sync.*
import std.time.*

/// ---- TCP mirror + delay/jitter/bandwidth relay ----
///
/// One frontend listener; every accepted connection connects to the
/// upstream; each direction passes through: mirror tee -> optional
/// delay/jitter sleep -> optional token-bucket bandwidth pacing -> write.
/// Byte stream integrity is preserved (no loss/dup on the TCP path —
/// that would corrupt stream semantics; datagram netem lives in the UDP
/// relay).

public class TcpMirrorProxy {
    let server: TcpServerSocket
    let stopFlag: AtomicBool
    public let listenPort: UInt16
    public let upstreamHost: String
    public let upstreamPort: UInt16
    public let mirror: ?MirrorSink
    public let delayMs: Int64
    public let jitterMs: Int64
    public let bucket: ?TokenBucket
    public let stats: RelayStats
    public let seed: UInt64

    public init(listenPort!: UInt16, upstreamHost!: String, upstreamPort!: UInt16,
                mirror!: ?MirrorSink, delayMs!: Int64, jitterMs!: Int64,
                bucket!: ?TokenBucket, stats!: RelayStats, seed!: UInt64 = 0) {
        this.server = TcpServerSocket(bindAt: listenPort)
        this.stopFlag = AtomicBool(false)
        this.listenPort = listenPort
        this.upstreamHost = upstreamHost
        this.upstreamPort = upstreamPort
        this.mirror = mirror
        this.delayMs = delayMs
        this.jitterMs = jitterMs
        this.bucket = bucket
        this.stats = stats
        this.seed = seed
    }

    public func start(): Unit {
        server.bind()
        let f = spawn {
            while (!stopFlag.load()) {
                var client: ?TcpSocket = None
                try {
                    client = Some(server.accept())
                } catch (_: Exception) {
                    break
                }
                match (client) {
                    case Some(c) =>
                        stats.addConnection()
                        spawn {
                            handleTcp(c, upstreamHost, upstreamPort,
                                mirror, delayMs, jitterMs, bucket, stats, seed)
                        }
                    case None => ()
                }
            }
        }
        let _ = f
    }

    public func stop(): Unit {
        stopFlag.store(true)
        try {
            server.close()
        } catch (_: Exception) {
            ()
        }
    }
}

public class RelayStats {
    public var connections: Int64 = 0
    public var c2uBytes: Int64 = 0
    public var u2cBytes: Int64 = 0
    let mutex: Mutex = Mutex()

    public func addConnection(): Unit {
        synchronized(mutex) { connections += 1 }
    }

    public func addC2u(n: Int64): Unit {
        synchronized(mutex) { c2uBytes += n }
    }

    public func addU2c(n: Int64): Unit {
        synchronized(mutex) { u2cBytes += n }
    }

    public func bytes(): (Int64, Int64) {
        synchronized(mutex) { (c2uBytes, u2cBytes) }
    }
}

func handleTcp(client: TcpSocket, upstreamHost: String, upstreamPort: UInt16,
               mirror: ?MirrorSink, delayMs: Int64, jitterMs: Int64,
               bucket: ?TokenBucket, stats: RelayStats, seed: UInt64): Unit {
    client.readTimeout = Duration.second * 10
    var up: ?TcpSocket = None
    try {
        let u = TcpSocket(upstreamHost, upstreamPort)
        u.connect(timeout: Duration.second * 10)
        u.readTimeout = Duration.second * 10
        up = Some(u)
    } catch (_: Exception) {
        try { client.close() } catch (_: Exception) { () }
        return
    }
    let upstream = up.getOrThrow()

    // per-direction jitter streams (shared rng across threads would race)
    let rngC2u = Xorshift64(seed: seed)
    let rngU2c = Xorshift64(seed: if (seed == 0) { UInt64(0x9E3779B9) } else { seed ^ UInt64(0xA5A5A5A5) })

    // client -> upstream (mirrored "C>U"), paced
    let f = spawn {
        let buf = Array<Byte>(65536, repeat: 0)
        while (true) {
            var n: Int64 = 0
            try {
                n = client.read(buf)
            } catch (_: Exception) {
                break
            }
            if (n <= 0) { break }
            let chunk = sliceOf(buf, n)
            match (mirror) {
                case Some(m) => m.record(nowEpochMs(), "C>U", chunk)
                case None => ()
            }
            stats.addC2u(Int64(n))
            try {
                pacedWrite(upstream, chunk, delayMs, jitterMs, bucket, rngC2u)
            } catch (_: Exception) {
                break
            }
        }
        try { upstream.close() } catch (_: Exception) { () }
    }
    let _ = f

    // upstream -> client (mirrored "U>C")
    let buf = Array<Byte>(65536, repeat: 0)
    while (true) {
        var n: Int64 = 0
        try {
            n = upstream.read(buf)
        } catch (_: Exception) {
            break
        }
        if (n <= 0) { break }
        let chunk = sliceOf(buf, n)
        match (mirror) {
            case Some(m) => m.record(nowEpochMs(), "U>C", chunk)
            case None => ()
        }
        stats.addU2c(Int64(n))
        try {
            pacedWrite(client, chunk, delayMs, jitterMs, bucket, rngU2c)
        } catch (_: Exception) {
            break
        }
    }
    try { client.close() } catch (_: Exception) { () }
    try { upstream.close() } catch (_: Exception) { () }
}

/// process-start monotonic anchor for bucket refill math
let NETEM_T0 = MonoTime.now()

func monoNowMs(): Int64 {
    let d = MonoTime.now() - NETEM_T0
    Int64(d / Duration.millisecond)
}

func pacedWrite(sock: TcpSocket, chunk: Array<Byte>, delayMs: Int64,
                jitterMs: Int64, bucket: ?TokenBucket, jitterRng: Xorshift64): Unit {
    var totalDelay = delayMs
    if (jitterMs > 0) {
        totalDelay += jitterRng.nextBelow(jitterMs + 1)
    }
    if (totalDelay > 0) {
        sleep(Duration.millisecond * totalDelay)
    }
    match (bucket) {
        case Some(b) =>
            var remaining = Int64(chunk.size)
            while (remaining > 0) {
                let now = monoNowMs()
                let granted = b.take(remaining, now)
                if (granted > 0) {
                    let piece = sliceOfBytes(chunk, chunk.size - Int64(remaining), granted)
                    sock.write(piece)
                    remaining -= granted
                } else {
                    sleep(Duration.millisecond * 1)
                }
            }
        case None =>
            sock.write(chunk)
    }
}

// helper: slice of a byte array
func sliceOfBytes(src: Array<Byte>, from: Int64, count: Int64): Array<Byte> {
    let out = ArrayList<Byte>()
    var i: Int64 = 0
    while (i < count) {
        out.add(src[from + i])
        i += 1
    }
    out.toArray()
}

func sliceOf(buf: Array<Byte>, n: Int64): Array<Byte> {
    sliceOfBytes(buf, 0, n)
}

func nowEpochMs(): Int64 {
    DateTime.now().toUnixTimeStamp().toMilliseconds()
}
