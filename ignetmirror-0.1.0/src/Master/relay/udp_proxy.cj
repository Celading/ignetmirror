package ignetmirror.Master.relay

import ignetmirror.Commons.*
import ignetmirror.Features.mirror.*
import ignetmirror.Features.netem.*
import std.collection.*
import std.net.*
import std.sync.*
import std.time.*

/// ---- UDP mirror + full netem relay (datagram semantics) ----
///
/// Two sockets: one bound to the client-facing port, one connected to
/// the upstream. Every datagram traversing either direction gets a
/// seeded netem decision (loss/dup/reorder/delay), is mirrored to the
/// sink with a direction tag, and is scheduled for release by the pump.
/// Upstream-bound datagrams leave via the connected upstream socket;
/// client-bound datagrams are sent back to the remembered client peer
/// (sendTo — the client socket stays unconnected to accept any peer).

public class UdpNetemRelay {
    public let clientPort: UInt16      // local port the client sends to
    public let upstreamAddr: IPSocketAddress
    let cfg: NetemConfig
    // one engine per direction: a shared engine would interleave draws
    // from two threads, making the seeded pattern non-reproducible
    let netemC2u: DatagramNetem
    let netemU2c: DatagramNetem
    let queue: ReleaseQueue
    let mirror: ?MirrorSink
    let bucket: ?TokenBucket
    let stopFlag: AtomicBool
    public var dgramSeen: Int64 = 0
    public var dgramDropped: Int64 = 0
    public var dgramDuplicated: Int64 = 0
    /// datagrams discarded because the bounded release window was full
    public var dgramQueueOverflow: Int64 = 0
    let mutex: Mutex = Mutex()

    // send endpoints (bound after construction)
    var clientSock: ?UdpSocket = None
    var upstreamSock: ?UdpSocket = None
    var clientPeer: ?IPSocketAddress = None

    public init(clientPort!: UInt16, upstreamAddr!: IPSocketAddress,
                cfg!: NetemConfig, mirror!: ?MirrorSink, seed!: UInt64,
                bucket!: ?TokenBucket = None<TokenBucket>,
                queueLimit!: Int64 = 1000) {
        this.clientPort = clientPort
        this.upstreamAddr = upstreamAddr
        this.cfg = cfg
        this.netemC2u = DatagramNetem(cfg: cfg, seed: seed)
        this.netemU2c = DatagramNetem(cfg: cfg,
            seed: if (seed == 0) { UInt64(0x9E3779B9) } else { seed ^ UInt64(0x5A5A5A5A5A5A5A5A) })
        this.mirror = mirror
        this.bucket = bucket
        this.queue = ReleaseQueue(maxPending: queueLimit)
        this.stopFlag = AtomicBool(false)
    }

    /// Bind both sockets, wire forwarding, start the release pump.
    public func start(): Bool {
        let c = UdpSocket(bindAt: clientPort)
        c.bind()
        clientSock = Some(c)
        let u = UdpSocket(bindAt: 0)
        u.bind()
        var uportVar: UInt16 = 0
        match (u.localAddress) {
            case a: IPSocketAddress => uportVar = a.port
            case _ => ()
        }
        if (uportVar == 0) { return false }
        // connected socket: send() targets upstream, receive is filtered
        // to upstream replies only
        u.connect(upstreamAddr)
        upstreamSock = Some(u)
        // immutable snapshots for the spawned lambdas
        let clientSockRef = clientSock
        let upstreamSockRef = upstreamSock

        // client -> [netem] -> upstream
        spawn {
            relayDirection(c, "C>U")
        }
        // upstream -> [netem] -> client
        spawn {
            relayDirection(u, "U>C")
        }
        // release pump for scheduled datagrams
        spawn {
            releasePump(clientSockRef, upstreamSockRef)
        }
        true
    }

    func relayDirection(src: UdpSocket, dirTag: String): Unit {
        src.receiveTimeout = Duration.millisecond * 200
        let buf = Array<Byte>(65507, repeat: 0)
        while (!stopFlag.load()) {
            var from: ?SocketAddress = None
            var n: Int64 = 0
            try {
                let (addr, count) = src.receiveFrom(buf)
                from = Some(addr)
                n = count
            } catch (_: Exception) {
                continue
            }
            if (n <= 0) { continue }
            let payload = udpSliceOf(buf, n)
            match (mirror) {
                case Some(m) => m.record(nowEpochMs(), dirTag, payload)
                case None => ()
            }
            synchronized(mutex) {
                dgramSeen += 1
            }
            // remember the client's address on the first datagram and
            // LOCK it: overwriting on every datagram would let any stray
            // sender redirect the whole return path
            if (dirTag == "C>U") {
                match (from) {
                    case Some(a) => rememberClient(a)
                    case None => ()
                }
            }
            let decision = if (dirTag == "C>U") { netemC2u.decide() } else { netemU2c.decide() }
            if (decision.drop) {
                synchronized(mutex) { dgramDropped += 1 }
                continue
            }
            let now = monoNowMs()
            let sendAt = now + decision.delayMs + decision.holdMs
            let target = if (dirTag == "C>U") { 1 } else { 0 }
            let pd = PendingDatagram(releaseAtMs: sendAt, target: target,
                payload: payload)
            if (queue.push(pd)) {
                if (decision.duplicate) {
                    synchronized(mutex) { dgramDuplicated += 1 }
                    if (!queue.push(pd)) {
                        synchronized(mutex) { dgramQueueOverflow += 1 }
                    }
                }
            } else {
                // declared overload policy: bounded pending window (tc
                // netem `limit` semantics) — drop and count, never grow
                synchronized(mutex) { dgramQueueOverflow += 1 }
            }
        }
    }

    func rememberClient(addr: SocketAddress): Unit {
        synchronized(mutex) {
            match (clientPeer) {
                case Some(_) => ()   // locked to the first client
                case None =>
                    match (addr) {
                        case a: IPSocketAddress => clientPeer = Some(a)
                        case _ => ()
                    }
            }
        }
    }

    func currentClientPeer(): ?IPSocketAddress {
        synchronized(mutex) { clientPeer }
    }

    func releasePump(clientSockRef: ?UdpSocket, upstreamSockRef: ?UdpSocket): Unit {
        while (!stopFlag.load()) {
            let due = queue.popDue(monoNowMs())
            for (item in due) {
                // bandwidth pacing: drain the bucket before releasing the
                // datagram (1 ms polling granularity, same as the TCP path)
                match (bucket) {
                    case Some(b) =>
                        var remaining = Int64(item.payload.size)
                        while (remaining > 0 && !stopFlag.load()) {
                            let granted = b.take(remaining, monoNowMs())
                            if (granted > 0) {
                                remaining -= granted
                            } else {
                                sleep(Duration.millisecond)
                            }
                        }
                    case None => ()
                }
                if (item.target == 1) {
                    match (upstreamSockRef) {
                        case Some(u) =>
                            try { u.send(item.payload) } catch (_: Exception) { () }
                        case None => ()
                    }
                } else {
                    match (currentClientPeer()) {
                        case Some(peer) =>
                            match (clientSockRef) {
                                case Some(c) =>
                                    try { c.sendTo(peer, item.payload) } catch (_: Exception) { () }
                                case None => ()
                            }
                        case None => ()
                    }
                }
            }
            sleep(Duration.millisecond)
        }
    }

    public func stop(): Unit {
        stopFlag.store(true)
        match (clientSock) {
            case Some(c) => try { c.close() } catch (_: Exception) { () }
            case None => ()
        }
        match (upstreamSock) {
            case Some(u) => try { u.close() } catch (_: Exception) { () }
            case None => ()
        }
    }
}

func udpSliceOf(buf: Array<Byte>, n: Int64): Array<Byte> {
    let out = ArrayList<Byte>()
    var i: Int64 = 0
    while (i < n) {
        out.add(buf[i])
        i += 1
    }
    out.toArray()
}
