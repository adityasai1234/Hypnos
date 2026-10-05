# Hypnos

Hypnos is one Linux daemon for hosting AI agents as actors. An idle actor is a SQLite file. A request wakes it, runs its JavaScript, then drops the guest from RAM after it has been idle. The host is Rust embedding Wasmtime and QuickJS (`rquickjs`) compiled to `wasm32-wasip1`. Guests write JavaScript. Do not wrap `workerd`.

Model calls go through an allow-listed `env.AI` binding. Local and remote models are both in scope. `env.AI.fetch` is a commit point: writes made before the call stay durable if the handler traps later.

The Arch PC wake measurement is in `RESULTS.md`. That run is the one that counts. `spike` is the measurement tool. `hypnos` is the daemon: `init`, `serve` on `127.0.0.1`, `deploy` for one actor script, and `meter`. `suite.sh` is the Debian container suite and is not how you run the daemon.

## On this Mac

This machine is the operator. The repo is already at `/Users/medow/Documents/Hypnos`. Run from that directory.

`cargo build -p spike` does not build the guest wasm. Build that first, or `spike run` fails with `No such file or directory` on `target/wasm32-wasip1/release/hypnos_guest.wasm`.

```bash
cd /Users/medow/Documents/Hypnos

cargo build -p hypnos-guest --target wasm32-wasip1 --release
cargo build -p spike --release

./target/release/spike sysinfo

./target/release/spike compile \
  target/wasm32-wasip1/release/hypnos_guest.wasm \
  -o engine/guest.cwasm

./target/release/spike run \
  --wasm target/wasm32-wasip1/release/hypnos_guest.wasm \
  --script scripts/counter.js \
  --root /tmp/run

./target/release/spike trap \
  --wasm target/wasm32-wasip1/release/hypnos_guest.wasm \
  --script scripts/counter.js \
  --root /tmp/trap

./target/release/spike agent \
  --wasm target/wasm32-wasip1/release/hypnos_guest.wasm \
  --script scripts/agent.js \
  --root /tmp/agent \
  --delay-ms 5000

./target/release/spike wake-bench \
  --wasm target/wasm32-wasip1/release/hypnos_guest.wasm \
  --script scripts/counter.js \
  --root /tmp/wake \
  --iters 20 \
  --cold-iters 3
```

Cold-cache lines print `SKIP` on macOS. That check needs Linux. The Arch wake table is already in `RESULTS.md`. Do not point another `wake-bench` at `/tmp/wake` on that machine. The first run's rows are still in that directory.

`192.168.1.4` is this Mac. Do not `scp` to that address. Do not use `/tmp/counter.js` or `/tmp/agent.js` on this Mac. Those paths are for the Arch machine after `scp`. On the Arch machine, run `hostname -I` and use the address it prints.

```bash
scp scripts/agent.js medow@ARCH:/tmp/agent.js
scp scripts/counter.js medow@ARCH:/tmp/counter.js
```

Replace `ARCH` with that address. Remote login has to be enabled on the Arch machine (`sudo systemctl enable --now sshd`).

## On the Arch Linux machine

Paste this only on Arch. This machine is the host. The repo is already checked out. The block above builds the spike. The daemon build is further down. Do not rerun `wake-bench` against `/tmp/wake`.

```bash
sudo pacman -S --needed base-devel curl pkgconf git

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
rustup target add wasm32-wasip1

cd ~/Hypnos

cargo build -p hypnos-guest --target wasm32-wasip1 --release
cargo build -p spike --release

./target/release/spike sysinfo

./target/release/spike run \
  --wasm target/wasm32-wasip1/release/hypnos_guest.wasm \
  --script scripts/counter.js \
  --root /tmp/run

./target/release/spike agent \
  --wasm target/wasm32-wasip1/release/hypnos_guest.wasm \
  --script scripts/agent.js \
  --root /tmp/agent \
  --delay-ms 5000
```

`spike run` prints one JSON line and then waits. Press Ctrl-C.

`suite.sh` is for the Debian container used to measure the spike. It `cd`s to `/src` and writes build output to `/opt/target`. Do not run it on the Arch machine. Use the `./target/release/spike` commands above when you want another measurement. Cold-cache numbers from this Mac do not count. The wake table that counts is the Arch section of `RESULTS.md`.

Build the daemon from the same checkout:

```bash
cargo build -p spike --release --bin hypnos

./target/release/hypnos init /var/lib/hypnos \
  --wasm target/wasm32-wasip1/release/hypnos_guest.wasm
./target/release/hypnos serve /var/lib/hypnos
```

`serve` listens on `127.0.0.1:8787` and loads `/var/lib/hypnos/guest.wasm`. Optional `/var/lib/hypnos/ai.allow` is one allow-list entry per line, `host[:port]=ENV_NAME`, with `#` comments skipped. The key stays in the environment. A missing file means the guest cannot call a model.

## Deploy from one laptop to another

One laptop is the operator. The other is the host. The host is the Linux machine. The operator never talks to Hypnos over the network.

The daemon listens on `127.0.0.1` only. SSH is the login. Do not bind Hypnos to `0.0.0.0` or to a LAN address so the other laptop can reach it directly. Guest code is untrusted, and the daemon has no credentials. An open port is an open proxy on a machine you pay the power bill for.

On the host:

```bash
hypnos init /var/lib/hypnos --wasm target/wasm32-wasip1/release/hypnos_guest.wasm
hypnos serve /var/lib/hypnos
```

From the operator laptop, each time the agent script changes:

```bash
scp ./agent.js host:/tmp/agent.js
ssh host -- hypnos deploy --name assistant --class actor \
  --source /tmp/agent.js
```

`hypnos serve` is already running on the host. The deploy command talks to the daemon on loopback, the same way it would if you were sitting at that machine.

To call the agent from the operator laptop, forward the loopback port and use your own machine:

```bash
ssh -L 8787:127.0.0.1:8787 host
curl http://127.0.0.1:8787/assistant/kitchen
```

The request still arrives on the host as a local connection. Nothing on the LAN can open Hypnos directly.

Counts already in the ledger, with no egress line and no prices:

```bash
ssh host -- hypnos meter /var/lib/hypnos
```

`serve` drops a guest from RAM after it has been idle for a second. A deploy sleeps that actor's live guest, so the next request wakes the new script without restarting the daemon. WebSocket streaming, the blob bucket, actor-to-actor calls, and an alarm index stay out until an agent needs them. A slow scan of many actor files is a signal for that index later. It is not a reason to stop using one SQLite file per actor on the wake path.
