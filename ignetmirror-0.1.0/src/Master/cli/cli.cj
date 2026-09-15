package ignetmirror.Master.cli

import ignetmirror.Commons.*
import ignetmirror.Features.mirror.*
import ignetmirror.Features.netem.*
import ignetmirror.Master.relay.*
import std.collection.*
import std.convert.*
import std.env.*
import std.net.*
import std.sync.*
import std.time.*

public const EXIT_OK: Int64 = 0
public const EXIT_FAIL: Int64 = 1
public const EXIT_USAGE: Int64 = 2

/// Duration-bounded run modes over the relay library:
///   mirror   — TCP tee only (no emulation)
///   netem    — TCP pacing + UDP datagram emulation (null mirror)
///   combined — tee + full emulation
///
/// The run is bounded by --duration-ms (default 60000), so the CLI needs
/// no signal handling to exit cleanly and is directly testable.

func usage(): Unit {
    println("ignetmirror <mode> [options]")
    println("modes: mirror | netem | combined")
    println("  --listen PORT             TCP frontend port (required)")
    println("  --upstream HOST:PORT      TCP upstream (required)")
    println("  --udp-listen PORT         UDP relay client port (netem/combined)")
    println("  --udp-upstream HOST:PORT  UDP upstream (default = --upstream)")
    println("  --mirror-dir DIR          file sink directory (mirror/combined)")
    println("  --delay-ms N --jitter-ms N --loss-pct N --dup-pct N")
    println("  --reorder-ms N --bandwidth-bps N --seed N --duration-ms N")
}

class CliOpts {
    public var mode: String = ""
    public var listen: UInt16 = 0
    public var upHost: String = "127.0.0.1"
    public var upPort: UInt16 = 0
    public var udpListen: UInt16 = 0
    public var udpUpHost: String = ""
    public var udpUpPort: UInt16 = 0
    public var mirrorDir: String = ""
    public var cfg: NetemConfig = NetemConfig()
    public var seed: UInt64 = 42
    public var durationMs: Int64 = 60000
}

func parseI64(v: String): ?Int64 {
    try { Some(Int64.parse(v)) } catch (_: Exception) { None<Int64> }
}

// String index yields bytes; host/port are ASCII-safe in practice.
func sliceBytes(s: String, from: Int64, to: Int64): String {
    let out = ArrayList<Byte>()
    var i: Int64 = from
    while (i < to && i < Int64(s.size)) {
        out.add(s[i])
        i += 1
    }
    String.fromUtf8(out.toArray())
}

func lastIndexOfColon(s: String): Int64 {
    var idx: Int64 = -1
    var i: Int64 = 0
    while (i < Int64(s.size)) {
        if (s[i] == UInt8(0x3A)) { idx = i }
        i += 1
    }
    idx
}

func splitHostPort(s: String): ?(String, UInt16) {
    let idx = lastIndexOfColon(s)
    if (idx <= 0 || idx >= Int64(s.size) - 1) { return None }
    match (parseI64(sliceBytes(s, idx + 1, Int64(s.size)))) {
        case Some(p) =>
            if (p > 0 && p <= 65535) {
                return Some((sliceBytes(s, 0, idx), UInt16(p)))
            }
        case None => ()
    }
    None
}

func setCfg(o: CliOpts, patch: (NetemConfig) -> NetemConfig): Unit {
    o.cfg = patch(o.cfg)
}

func parseArgs(args: ArrayList<String>): ?CliOpts {
    let o = CliOpts()
    if (args.size < 1) { return None }
    o.mode = args[0]
    if (o.mode != "mirror" && o.mode != "netem" && o.mode != "combined") {
        return None
    }
    var i: Int64 = 1
    while (i < args.size) {
        if (i + 1 >= args.size) { return None }
        let flag = args[i]
        let v = args[i + 1]
        i += 2
        match (flag) {
            case "--listen" =>
                match (parseI64(v)) {
                    case Some(n) => if (n > 0 && n <= 65535) { o.listen = UInt16(n) }
                    case None => return None
                }
            case "--upstream" =>
                match (splitHostPort(v)) {
                    case Some(pair) =>
                        o.upHost = pair[0]
                        o.upPort = pair[1]
                    case None => return None
                }
            case "--udp-listen" =>
                match (parseI64(v)) {
                    case Some(n) => if (n > 0 && n <= 65535) { o.udpListen = UInt16(n) }
                    case None => return None
                }
            case "--udp-upstream" =>
                match (splitHostPort(v)) {
                    case Some(pair) =>
                        o.udpUpHost = pair[0]
                        o.udpUpPort = pair[1]
                    case None => return None
                }
            case "--mirror-dir" => o.mirrorDir = v
            case "--delay-ms" =>
                setCfg(o, { c => NetemConfig(delayMs: maxI64(num(v, c), 0), jitterMs: c.jitterMs, lossPct: c.lossPct, dupPct: c.dupPct, reorderMs: c.reorderMs, bandwidthBps: c.bandwidthBps) })
            case "--jitter-ms" =>
                setCfg(o, { c => NetemConfig(delayMs: c.delayMs, jitterMs: if (num(v, c) < 0) { 0 } else { num(v, c) }, lossPct: c.lossPct, dupPct: c.dupPct, reorderMs: c.reorderMs, bandwidthBps: c.bandwidthBps) })
            case "--loss-pct" =>
                setCfg(o, { c => NetemConfig(delayMs: c.delayMs, jitterMs: c.jitterMs, lossPct: clampPct(num(v, c)), dupPct: c.dupPct, reorderMs: c.reorderMs, bandwidthBps: c.bandwidthBps) })
            case "--dup-pct" =>
                setCfg(o, { c => NetemConfig(delayMs: c.delayMs, jitterMs: c.jitterMs, lossPct: c.lossPct, dupPct: clampPct(num(v, c)), reorderMs: c.reorderMs, bandwidthBps: c.bandwidthBps) })
            case "--reorder-ms" =>
                setCfg(o, { c => NetemConfig(delayMs: c.delayMs, jitterMs: c.jitterMs, lossPct: c.lossPct, dupPct: c.dupPct, reorderMs: if (num(v, c) < 0) { 0 } else { num(v, c) }, bandwidthBps: c.bandwidthBps) })
            case "--bandwidth-bps" =>
                setCfg(o, { c => NetemConfig(delayMs: c.delayMs, jitterMs: c.jitterMs, lossPct: c.lossPct, dupPct: c.dupPct, reorderMs: c.reorderMs, bandwidthBps: if (num(v, c) < 0) { 0 } else { num(v, c) }) })
            case "--seed" =>
                match (parseI64(v)) {
                    case Some(n) => if (n >= 0) { o.seed = UInt64(n) }
                    case None => return None
                }
            case "--duration-ms" =>
                match (parseI64(v)) {
                    case Some(n) => if (n > 0 && n <= 3600000) { o.durationMs = n }
                    case None => return None
                }
            case _ => return None
        }
    }
    if (o.listen == 0 || o.upPort == 0) { return None }
    if (o.udpListen > 0 && o.udpUpPort == 0) {
        o.udpUpHost = o.upHost
        o.udpUpPort = o.upPort
    }
    Some(o)
}

// lambda-friendly numeric fetch: unparseable -> -1 (rejected/clamped per flag)
func num(v: String, c: NetemConfig): Int64 {
    match (parseI64(v)) {
        case Some(n) => n
        case None => -1
    }
}

func maxI64(a: Int64, b: Int64): Int64 {
    if (a > b) { a } else { b }
}

/// Run one bounded CLI session; returns process exit code.
public func run(args: ArrayList<String>): Int64 {
    let opts = match (parseArgs(args)) {
        case Some(o) => o
        case None =>
            usage()
            return EXIT_USAGE
    }
    let sink: ?MirrorSink = if (opts.mode == "netem" || opts.mirrorDir == "") {
        None
    } else {
        Some(MirrorSink(dir: opts.mirrorDir))
    }
    let stats = RelayStats()
    let bucket: ?TokenBucket = if (opts.cfg.bandwidthBps > 0) {
        Some(TokenBucket(rateBps: opts.cfg.bandwidthBps,
            capacityBytes: if (opts.cfg.bandwidthBps > 65536) { 65536 } else { opts.cfg.bandwidthBps }))
    } else {
        None<TokenBucket>
    }
    let proxy = TcpMirrorProxy(listenPort: opts.listen,
        upstreamHost: opts.upHost, upstreamPort: opts.upPort,
        mirror: sink, delayMs: opts.cfg.delayMs, jitterMs: opts.cfg.jitterMs,
        bucket: bucket, stats: stats, seed: opts.seed)
    var udp: ?UdpNetemRelay = None
    try {
        proxy.start()
    } catch (_: Exception) {
        println("error: cannot bind TCP listen port ${opts.listen}")
        return EXIT_FAIL
    }
    if (opts.udpListen > 0) {
        let uaddr = IPSocketAddress.parse("${opts.udpUpHost}:${opts.udpUpPort}")
        let relay = UdpNetemRelay(clientPort: opts.udpListen,
            upstreamAddr: uaddr, cfg: opts.cfg, mirror: sink,
            seed: opts.seed, bucket: bucket)
        if (relay.start()) {
            udp = Some(relay)
        } else {
            println("error: cannot bind UDP listen port ${opts.udpListen}")
            proxy.stop()
            return EXIT_FAIL
        }
    }
    let udpNote = if (opts.udpListen > 0) { " udp ${opts.udpListen}" } else { "" }
    println("ignetmirror ${opts.mode} running: tcp ${opts.listen}->${opts.upHost}:${opts.upPort}${udpNote} for ${opts.durationMs}ms")
    sleep(Duration.millisecond * opts.durationMs)
    proxy.stop()
    match (udp) {
        case Some(r) => r.stop()
        case None => ()
    }
    // let in-flight relay threads flush the sink before closing it
    sleep(Duration.millisecond * 200)
    match (sink) {
        case Some(s) => s.close()
        case None => ()
    }
    let (c2u, u2c) = stats.bytes()
    let udpStats = match (udp) {
        case Some(r) => " dgramSeen=${r.dgramSeen} dropped=${r.dgramDropped} dup=${r.dgramDuplicated}"
        case None => ""
    }
    println("stats: connections=${stats.connections} c2uBytes=${c2u} u2cBytes=${u2c}${udpStats}")
    EXIT_OK
}
