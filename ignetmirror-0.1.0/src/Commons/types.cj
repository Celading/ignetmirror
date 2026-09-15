package ignetmirror.Commons

import std.collection.*
import std.net.*

/// Deterministic xorshift64 PRNG (in-member): netem loss/dup/reorder
/// patterns must be reproducible from a seed.
public class Xorshift64 {
    private var state: UInt64

    public init(seed!: UInt64) {
        this.state = if (seed == 0) { UInt64(0x9E3779B9) } else { seed }
    }

    public func nextU64(): UInt64 {
        var x = state
        x ^= x << 13
        x ^= x >> 7
        x ^= x << 17
        state = x
        x
    }

    /// value in [lo, hi)
    public func nextBelow(hi: Int64): Int64 {
        if (hi <= 0) { return 0 }
        Int64(nextU64() % UInt64(hi))
    }
}

/// netem profile (all optional; zeros = off).
public struct NetemConfig {
    public let delayMs: Int64        // constant delay per datagram/chunk
    public let jitterMs: Int64       // uniform 0..jitter added to delay
    public let lossPct: Int64        // 0..100 (datagram mode)
    public let dupPct: Int64         // 0..100 (datagram mode)
    public let reorderMs: Int64      // hold window producing out-of-order (UDP)
    public let bandwidthBps: Int64   // token-bucket cap (0 = unlimited)

    public init(delayMs!: Int64 = 0, jitterMs!: Int64 = 0, lossPct!: Int64 = 0,
                dupPct!: Int64 = 0, reorderMs!: Int64 = 0, bandwidthBps!: Int64 = 0) {
        this.delayMs = delayMs
        this.jitterMs = jitterMs
        this.lossPct = lossPct
        this.dupPct = dupPct
        this.reorderMs = reorderMs
        this.bandwidthBps = bandwidthBps
    }
}

public func clampPct(v: Int64): Int64 {
    if (v < 0) { return 0 }
    if (v > 100) { return 100 }
    v
}

/// Free loopback port (bind 0, read, release — small race, acceptable).
public func pickFreePort(): UInt16 {
    let server = TcpServerSocket(bindAt: UInt16(0))
    var picked: UInt16 = 0
    server.bind()
    match (server.localAddress) {
        case a: IPSocketAddress => picked = a.port
        case _ => picked = 0
    }
    server.close()
    picked
}

