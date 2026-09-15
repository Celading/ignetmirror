package ignetmirror.Features.netem

import ignetmirror.Commons.*
import std.collection.*
import std.sync.*
import std.time.*

/// Token bucket for bandwidth emulation (bytes/sec, burst = capacity).
public class TokenBucket {
    let rateBps: Int64
    let capacity: Int64
    var tokens: Int64
    /// Sub-token remainder in byte·ms (an earlier audit): refills carry it instead of
    /// truncating, so N small steps grant exactly what one equal-total
    /// big step grants. 100 B/s polled every 1 ms used to refill 0
    /// forever (100×1/1000 = 0 every call, remainder discarded).
    var carry: Int64
    let mutex: Mutex = Mutex()
    var lastRefillMs: Int64

    public init(rateBps!: Int64, capacityBytes!: Int64) {
        this.rateBps = rateBps
        this.capacity = capacityBytes
        this.tokens = capacityBytes
        this.carry = 0
        this.lastRefillMs = 0
    }

    /// Take up to `want` tokens; returns how many are available now
    /// (caller sleeps for the deficit and retries).
    public func take(want: Int64, nowMs: Int64): Int64 {
        synchronized(mutex) {
            let elapsed = nowMs - lastRefillMs
            if (elapsed > 0 && rateBps > 0) {
                if (elapsed > 9223372036854775807 / rateBps) {
                    // span so long the bucket saturates regardless of
                    // remainder — clamp instead of overflowing
                    tokens = capacity
                    carry = 0
                } else {
                    let produced = elapsed * rateBps
                    tokens += produced / 1000
                    carry += produced % 1000
                    if (carry >= 1000) {
                        tokens += carry / 1000
                        carry %= 1000
                    }
                }
                if (tokens > capacity) { tokens = capacity }
                lastRefillMs = nowMs
            }
            // elapsed <= 0 (clock regression / same-ms call): no refill,
            // stamp unchanged so a later forward step covers the span
            let granted = if (tokens < want) { tokens } else { want }
            tokens -= granted
            granted
        }
    }
}

/// Per-datagram netem decision (loss / duplication / reorder / delay).
public class NetemDecision {
    public let drop: Bool
    public let duplicate: Bool
    public let delayMs: Int64
    /// hold window for reorder (datagram is released after a random
    /// portion of this window)
    public let holdMs: Int64

    public init(drop!: Bool, duplicate!: Bool, delayMs!: Int64, holdMs!: Int64) {
        this.drop = drop
        this.duplicate = duplicate
        this.delayMs = delayMs
        this.holdMs = holdMs
    }
}

/// Seeded datagram netem decision engine.
public class DatagramNetem {
    let cfg: NetemConfig
    let rng: Xorshift64

    public init(cfg!: NetemConfig, seed!: UInt64) {
        this.cfg = cfg
        this.rng = Xorshift64(seed: seed)
    }

    public func decide(): NetemDecision {
        let roll = rng.nextBelow(100)
        let drop = roll < clampPct(cfg.lossPct)
        let duplicate = rng.nextBelow(100) < clampPct(cfg.dupPct)
        var delay = cfg.delayMs
        if (cfg.jitterMs > 0) {
            delay += rng.nextBelow(cfg.jitterMs + 1)
        }
        var hold: Int64 = 0
        if (cfg.reorderMs > 0) {
            hold = rng.nextBelow(cfg.reorderMs + 1)
        }
        NetemDecision(drop: drop, duplicate: duplicate,
            delayMs: delay, holdMs: hold)
    }
}

/// A datagram scheduled for future release.
public struct PendingDatagram {
    public let releaseAtMs: Int64
    public let target: Int64        // 0 = client side, 1 = upstream side
    public let payload: Array<Byte>

    public init(releaseAtMs!: Int64, target!: Int64, payload!: Array<Byte>) {
        this.releaseAtMs = releaseAtMs
        this.target = target
        this.payload = payload
    }
}

/// Time-ordered release queue with a 1 ms polling granularity.
/// Bounded: when `maxPending` datagrams are already held, push reports
/// overflow (returns false) and the caller drops — the tc netem `limit`
/// semantics (queue-full == drop), so a producer faster than the release
/// side can never grow the relay's memory without bound.
public class ReleaseQueue {
    let mutex: Mutex = Mutex()
    var items: ArrayList<PendingDatagram> = ArrayList<PendingDatagram>()
    public let maxPending: Int64

    public init(maxPending!: Int64 = 1000) {
        this.maxPending = maxPending
    }

    /// Enqueue; false = queue at capacity, caller must drop the datagram.
    public func push(item: PendingDatagram): Bool {
        synchronized(mutex) {
            if (Int64(items.size) >= maxPending) {
                false
            } else {
                // keep sorted by releaseAtMs; ArrayList lacks insert — rebuild
                // the list around the insertion point (queue stays small)
                let nextItems = ArrayList<PendingDatagram>()
                var placed = false
                var k: Int64 = 0
                while (k < items.size) {
                    if (!placed && items[k].releaseAtMs > item.releaseAtMs) {
                        nextItems.add(item)
                        placed = true
                    }
                    nextItems.add(items[k])
                    k += 1
                }
                if (!placed) {
                    nextItems.add(item)
                }
                items = nextItems
                true
            }
        }
    }

    /// Pop all items whose release time has come (nowMs).
    public func popDue(nowMs: Int64): ArrayList<PendingDatagram> {
        synchronized(mutex) {
            let due = ArrayList<PendingDatagram>()
            while (items.size > 0 && items[0].releaseAtMs <= nowMs) {
                due.add(items[0])
                items.remove(at: 0)
            }
            due
        }
    }

    public func size(): Int64 {
        synchronized(mutex) { Int64(items.size) }
    }
}
