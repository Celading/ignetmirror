# ignetmirror

Deterministic network emulation and mirror tee in pure Cangjie standard library: seeded drop/duplicate/delay/reorder decisions (Xorshift64), token-bucket bandwidth caps, bounded release queues with due-time ordering, and a mirror sink for tee-ing datagram streams to observers.

## Scope

- `DatagramNetem`: per-datagram decisions driven entirely by the seed — same seed and load reproduces the same decisions.
- `TokenBucket`: byte·ms remainder-preserving rate accounting.
- `ReleaseQueue`: bounded pending-datagram release in due order; pushes past capacity are refused, never silently dropped.
- Real relay layer (distinct from the model): `TcpMirrorProxy` mirrors live TCP client/server byte streams both directions with per-direction accounting, and `UdpNetemRelay` applies the emulation decisions to real UDP traffic — the model loop and the live-forwarding path are separate entry points.

The library forwards real sockets through the relay layer and models impairment decisions deterministically; it does not capture third-party processes' traffic.

## Requirements and build

Verified with Cangjie/CJPM 1.1.3 on macOS arm64. From the source repository root:

```sh
cjpm build -j1
./target/release/bin/ignetmirror_test
```

## Use from a separate project

Keep the package beside your consumer and declare `ignetmirror = { path = "../ignetmirror-0.1.0" }`. Consumer `src/main.cj`:

```cangjie
package netmirror_example

import ignetmirror.Features.netem.*
import ignetmirror.Commons.*

main(): Int64 {
    let netem = DatagramNetem(cfg: NetemConfig(lossPct: 100), seed: 42)
    let d = netem.decide()
    if (!d.drop) { return 1 }
    println("netem decision: drop=${d.drop} delay_ms=${d.delayMs}")
    0
}
```

Expected output:

```text
netem decision: drop=true delay_ms=0
```

## Errors and limits

Percent parameters are clamped to 0..100. Queue capacity bounds pending memory; an application must pop due datagrams or pushes are refused. Two runs with the same seed and load produce identical decision sequences — that determinism is the contract, not an accident.

This is user-space emulation, not a replacement for kernel netem or a congestion-control implementation.

## Source preview layout

The repository contains the library at `ignetmirror-0.1.0/` and executable projects `ignetmirror_demo/`, `ignetmirror_test/`. The versioned directory name, where present, is retained for path compatibility; this source update is not a new registry release.

Run the documented build and tests from the repository root. A standalone extracted library package contains only the library: run `cjpm build -j1` there, then use the separate-project example above. It does not contain the repository test executables.

The suite may create capture files or local test data in its working directory. Run each checkout serially in a disposable directory. Public-network tests and long-running soak modes are not enabled by default. A successful unit run does not establish full interoperability or production certification.

## License

See [LICENSE](LICENSE) and [NOTICE](NOTICE). Third-party notices, when present, remain separately applicable.
