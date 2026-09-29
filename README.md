# VPFS

A versioned peer-to-peer file system built with [Iroh](https://github.com/n0-computer/iroh) (QUIC). This repository builds three binaries: the VPFS daemon, a demo shell, and a conflict resolver. The daemon must be running on a machine before any other program can interact with the file system on that machine.

## Building

```sh
cargo build --release
```

## Usage

### Daemon

```sh
cargo run --bin daemon -- -n <name> [options]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-n, --name <name>` | Name for this node. Must be unique across the system and consistent across restarts. | *(required)* |
| `-p, --port <port>` | Iroh (QUIC) port for peer-to-peer communication. | `8081` |
| `-l, --listen-port <port>` | TCP port for connections from local client programs. | `8082` |
| `-c, --conflict-port <port>` | TCP port used to contact the conflict resolver on concurrent modifications. | `8083` |
| `--remote-id <pubkey>` | Iroh public key of an existing node to connect to. Omit to start a new network. | — |
| `-s, --cache-size <bytes>` | Maximum size of the local file cache in bytes. | `65536` |

The daemon persists state across restarts in the `./files/` directory: a `log`, a `file_system` snapshot, a `cache` state file, and any files owned or cached by the node.

### Shell

A basic demo shell for testing VPFS functionality.

```sh
cargo run --bin sh [-- options]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-p, --port <port>` | Listen port of the local VPFS daemon. | `8082` |

The shell supports input redirection (`<`), output redirection (`>`), and pipes (`|`). External programs can be launched from the shell — VPFS paths are resolved before the program starts, so any program that reads from stdin or writes to stdout can interact with VPFS files through these mechanisms.

### Conflict Resolver

The conflict resolver runs independently of the daemon and must be started separately.

```sh
cargo run --bin conflict_resolver [-- options]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-p, --port <port>` | Port to listen on for connections from the local daemon. Must match the daemon's `--conflict-port`. | `8083` |

## Architecture

```
client programs ──TCP──> gateway ─────┐
other daemons ──iroh──> peer_handler ─┴─> service ──> executor ──> state
                                           │                         ├─ namespace  (path -> FileEntry)
                                           ├─> transport (peers)     ├─ logbook    (log + vector clock)
                                           └─> human (resolver)      ├─ cache      (LRU copies of remote files)
                                                                     └─ blobs      (content on disk)
```

| Module (`src/daemon/`) | Responsibility |
|---|---|
| `gateway.rs`, `peer_handler.rs` | Decode requests from clients / other daemons, call the service. No logic. |
| `service.rs` | Route each operation: local state (via executor) or the owner node (via transport); deliver effects. |
| `executor.rs` | Task queue owning the state. The only place that decides how tasks are scheduled. |
| `state.rs` | The only code that changes the node's state. Local ops and remote events converge here. |
| `content.rs` | Write policy per `FileKind` (`Blob`: replace only; `Text`: `InsertAt`/`DeleteAt`). |
| `conflict.rs` | Conflict heuristics per kind; unresolved conflicts quarantine the path until a human decides. |
| `transport.rs`, `membership.rs` | Delivery to peers (`fetch`, `broadcast`) and join/hello handshakes. |

Shared with clients (`src/`): `messages.rs` (all wire and on-disk types), `framing.rs`, `client.rs`, `ffi.rs`.

## Testing

`legacy/` is a frozen copy of the pre-refactor implementation, kept as a behavioural oracle.
The e2e suites in `tests/` compile against both; assertions on behaviour fixed by the refactor branch on `LEGACY`.

```sh
cargo test                                   # current implementation
cargo test --manifest-path legacy/Cargo.toml # oracle
```

Two-node tests need network access (iroh discovery).
