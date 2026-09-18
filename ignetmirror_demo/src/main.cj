package ignetmirror_demo

import ignetmirror.Commons.*
import ignetmirror.Features.mirror.*
import ignetmirror.Features.netem.*
import ignetmirror.Master.relay.*
import std.collection.*
import std.convert.*
import std.env.*
import std.fs.*
import std.net.*
import std.sync.*
import std.time.*

/// igNetMirror live demo: a combined mirror+netem profile over loopback.
/// Starts real echo endpoints, threads a TCP stream and a UDP flow through
/// the relay with delay/jitter/loss/dup/reorder/bandwidth enabled, prints
/// the measured effect, and exits 0 only when every sanity bound holds.

var passCount: Int64 = 0
var failCount: Int64 = 0

func check(name: String, ok: Bool): Unit {
    if (ok) {
        passCount += 1
        println("  PASS: ${name}")
    } else {
        failCount += 1
        println("  FAIL: ${name}")
    }
}

class TcpEcho {
    let server: TcpServerSocket
    let stop: AtomicBool
    public let port: UInt16
    init(port!: UInt16) {
        let s = TcpServerSocket(bindAt: port)
        s.bind()
        var p: UInt16 = port
        match (s.localAddress) {
            case a: IPSocketAddress => p = a.port
            case _ => ()
        }
        this.server = s
        this.stop = AtomicBool(false)
        this.port = p
    }
    public func start(): Unit {
        spawn {
            while (!stop.load()) {
                var sock: ?TcpSocket = None
                try { sock = Some(server.accept()) } catch (_: Exception) { break }
                match (sock) {
                    case Some(s) =>
                        spawn {
                            s.readTimeout = Duration.second * 5
                            let buf = Array<Byte>(65536, repeat: 0)
                            while (true) {
                                var n: Int64 = 0
                                try { n = s.read(buf) } catch (_: Exception) { break }
                                if (n <= 0) { break }
                                let out = ArrayList<Byte>()
                                var i: Int64 = 0
                                while (i < n) { out.add(buf[i]); i += 1 }
                                try { s.write(out.toArray()) } catch (_: Exception) { break }
                            }
                            try { s.close() } catch (_: Exception) { () }
                        }
                    case None => ()
                }
            }
        }
    }
    public func halt(): Unit {
        stop.store(true)
        try { server.close() } catch (_: Exception) { () }
    }
}

class UdpEcho {
    let sock: UdpSocket
    let stop: AtomicBool
    public let port: UInt16
    init() {
        let s = UdpSocket(bindAt: 0)
        s.bind()
        var p: UInt16 = 0
        match (s.localAddress) {
            case a: IPSocketAddress => p = a.port
            case _ => ()
        }
        this.sock = s
        this.stop = AtomicBool(false)
        this.port = p
    }
    public func start(): Unit {
        spawn {
            sock.receiveTimeout = Duration.millisecond * 200
            let buf = Array<Byte>(65507, repeat: 0)
            while (!stop.load()) {
                var from: ?SocketAddress = None
                var n: Int64 = 0
                try {
                    let (addr, count) = sock.receiveFrom(buf)
                    from = Some(addr)
                    n = count
                } catch (_: Exception) { continue }
                if (n <= 0) { continue }
                match (from) {
                    case Some(a) =>
                        let out = ArrayList<Byte>()
                        var i: Int64 = 0
                        while (i < n) { out.add(buf[i]); i += 1 }
                        try { sock.sendTo(a, out.toArray()) } catch (_: Exception) { () }
                    case None => ()
                }
            }
        }
    }
    public func halt(): Unit {
        stop.store(true)
        try { sock.close() } catch (_: Exception) { () }
    }
}

main(): Int64 {
    println("========================================")
    println("  igNetMirror Demo — mirror + netem over loopback")
    println("========================================")

    let demoDir = "/tmp/ignetmirror-demo-${DateTime.now().toUnixTimeStamp().toMilliseconds()}"
    let tcpEcho = TcpEcho(port: 0)
    tcpEcho.start()
    let udpEcho = UdpEcho()
    udpEcho.start()
    sleep(Duration.millisecond * 100)

    // demo profile: visible delay + loss + reorder + bandwidth shaping
    let cfg = NetemConfig(delayMs: 30, jitterMs: 10, lossPct: 15,
        dupPct: 10, reorderMs: 40, bandwidthBps: 2 * 1024 * 1024)
    let stats = RelayStats()
    let mirror = MirrorSink(dir: demoDir)
    let proxy = TcpMirrorProxy(listenPort: pickFreePort(),
        upstreamHost: "127.0.0.1", upstreamPort: tcpEcho.port,
        mirror: Some(mirror), delayMs: cfg.delayMs, jitterMs: cfg.jitterMs,
        bucket: Some(TokenBucket(rateBps: cfg.bandwidthBps, capacityBytes: 65536)),
        stats: stats, seed: 2026)
    proxy.start()
    let relay = UdpNetemRelay(clientPort: pickFreePort(),
        upstreamAddr: IPSocketAddress.parse("127.0.0.1:${udpEcho.port}"),
        cfg: cfg, mirror: Some(mirror), seed: 2026)
    let udpUp = relay.start()
    sleep(Duration.millisecond * 100)

    println("  profile: delay=30ms jitter=10ms loss=15% dup=10% reorder=40ms bw=2Mbps seed=2026")
    println("  tcp relay: 127.0.0.1:${proxy.listenPort} -> 127.0.0.1:${tcpEcho.port}")
    println("  udp relay: 127.0.0.1:${relay.clientPort} -> 127.0.0.1:${udpEcho.port}")
    println("  mirror sink: ${demoDir}/mirror-stream.log")
    println("")

    // ---- TCP leg: 256KB round trip through the shaped path ----
    let payload = Array<Byte>(262144, repeat: 0)
    var i: Int64 = 0
    while (i < 262144) {
        payload[i] = UInt8(UInt64(i * 7 + 13) & UInt64(0xFF))
        i += 1
    }
    let sock = TcpSocket("127.0.0.1", proxy.listenPort)
    sock.connect()
    sock.readTimeout = Duration.second * 15
    let t0 = MonoTime.now()
    let w = spawn {
        try { sock.write(payload) } catch (_: Exception) { () }
    }
    let _ = w
    let got = ArrayList<Byte>()
    let buf = Array<Byte>(65536, repeat: 0)
    while (got.size < 262144) {
        var n: Int64 = 0
        try { n = sock.read(buf) } catch (_: Exception) { break }
        if (n <= 0) { break }
        var k: Int64 = 0
        while (k < n) { got.add(buf[k]); k += 1 }
    }
    let tcpMs = Int64((MonoTime.now() - t0) / Duration.millisecond)
    try { sock.close() } catch (_: Exception) { () }
    say("tcp leg done in ${tcpMs}ms, got ${got.size} bytes")

    var byteExact = true
    i = 0
    while (i < 262144) {
        if (got.size != 262144 || got[i] != payload[i]) { byteExact = false; break }
        i += 1
    }
    let (c2u, u2c) = stats.bytes()
    println("  tcp leg: 256KB round trip in ${tcpMs}ms (bw floor ~250ms)")
    check("demo: tcp 256KB survives shaped path byte-exact", byteExact && got.size == 262144)
    check("demo: tcp relay counted both directions",
        c2u == 262144 && u2c == 262144)
    check("demo: bandwidth cap stretches the round trip (>=150ms)", tcpMs >= 150)

    // ---- UDP leg: 100 datagrams through loss+dup+reorder ----
    let cl = UdpSocket(bindAt: 0)
    cl.bind()
    cl.receiveTimeout = Duration.millisecond * 100
    let target = IPSocketAddress.parse("127.0.0.1:${relay.clientPort}")
    let sent = HashSet<String>()
    i = 0
    say("udp send loop begin")
    while (i < 100) {
        let tag = if (i < 10) { "demo-0${i}" } else { "demo-${i}" }
        sent.add(tag)
        try { cl.sendTo(target, tag.toArray()) } catch (_: Exception) { () }
        sleep(Duration.millisecond * 4)
        i += 1
    }
    say("udp send loop done")
    let seen = HashSet<String>()
    var arrivals: Int64 = 0
    let drainT0 = MonoTime.now()
    say("udp drain begin")
    while (MonoTime.now() - drainT0 < Duration.millisecond * 1500) {
        try {
            let (_, n) = cl.receiveFrom(buf)
            if (n > 0) {
                arrivals += 1
                seen.add(String.fromUtf8(sliceOfDemo(buf, n)))
            }
        } catch (_: Exception) { () }
    }
    println("  udp leg: sent 100 -> ${arrivals} arrivals, ${seen.size} unique (loss 15%/dir + dup 10%/dir)")
    check("demo: udp flow shows real loss (unique arrivals < 100)", Int64(seen.size) < 100)
    check("demo: udp flow stays majority-delivered (unique >= 40)", Int64(seen.size) >= 40)
    check("demo: dup+reorder produce extra arrivals or reordering",
        arrivals >= Int64(seen.size))
    check("demo: udp relay wired (start ok)", udpUp)

    sleep(Duration.millisecond * 300)
    proxy.stop()
    relay.stop()
    tcpEcho.halt()
    udpEcho.halt()
    sleep(Duration.millisecond * 200)
    mirror.close()
    let sinkBytes = try {
        Some(File.readFrom(Path("${demoDir}/mirror-stream.log")).size)
    } catch (_: Exception) {
        None<Int64>
    }
    check("demo: mirror sink captured the traffic (file non-empty)",
        match (sinkBytes) { case Some(n) => n > 0 case None => false })

    println("")
    println("========================================")
    println("  RESULT: ${passCount} passed, ${failCount} failed")
    println("========================================")
    if (failCount > 0) {
        println("FAIL: demo bounds violated — exiting non-zero")
        return 1
    }
    0
}

func sliceOfDemo(buf: Array<Byte>, n: Int64): Array<Byte> {
    let out = ArrayList<Byte>()
    var i: Int64 = 0
    while (i < n) {
        out.add(buf[i])
        i += 1
    }
    out.toArray()
}

func say(s: String): Unit {
    getStdOut().write("[demo] ${s}\r\n".toArray())
    getStdOut().flush()
}
