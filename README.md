# Hypnos

Hypnos is one Linux daemon for hosting AI agents as actors. An idle actor is a SQLite file. A request wakes it, runs its JavaScript, then drops the guest from RAM after it has been idle. The host is Rust embedding Wasmtime and QuickJS (`rquickjs`) compiled to `wasm32-wasip1`. Guests write JavaScript. Do not wrap `workerd`.

Model calls go through an allow-listed `env.AI` binding. Local and remote models are both in scope. `env.AI.fetch` is a commit point: writes made before the call stay durable if the handler traps later.

The code in this repo is the measurement spike, not the daemon. There is no `hypnos serve` and no `hypnos deploy`. Spike numbers are provisional until the same wake bench has been run on the real Linux box.

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

Cold-cache lines print `SKIP` on macOS. That check needs Linux.

`192.168.1.4` is this Mac. Do not `scp` to that address. Do not use `/tmp/counter.js` or `/tmp/agent.js` on this Mac. Those paths are for the Arch machine after `scp`. On the Arch machine, run `hostname -I` and use the address it prints.

```bash
scp scripts/agent.js medow@ARCH:/tmp/agent.js
scp scripts/counter.js medow@ARCH:/tmp/counter.js
```

Replace `ARCH` with that address. Remote login has to be enabled on the Arch machine (`sudo systemctl enable --now sshd`).

## On the Arch Linux machine

Paste this only on Arch. This machine is the host. The repo is already checked out. There is no `hypnos` command.

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

`suite.sh` is for the Debian container used to measure the spike. It `cd`s to `/src` and writes build output to `/opt/target`. Do not run it on the Arch machine. Use the `./target/release/spike` commands above. Cold-cache numbers from this Mac do not count. Repeat `wake-bench` on the Arch machine when you want the number that matters.

## Deploy from one laptop to another

One laptop is the operator. The other is the host. The host is the Linux machine. The operator never talks to Hypnos over the network.

The daemon listens on `127.0.0.1` only. SSH is the login. Do not bind Hypnos to `0.0.0.0` or to a LAN address so the other laptop can reach it directly. Guest code is untrusted, and the daemon has no credentials. An open port is an open proxy on a machine you pay the power bill for.

On the host, once the daemon exists:

```bash
hypnos init /var/lib/hypnos
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

## Until `hypnos serve` exists

From the existing checkout on the Linux laptop, run the spike there. That runs one bench, then exits. It does not keep an agent deployed.

```bash
bash suite.sh
```

The first product slice, after the wake bench has been repeated on that box, is:

- `hypnos init` and `hypnos serve` on `127.0.0.1`
- `hypnos deploy` for one actor script, loaded without a restart
- One path, such as `/assistant/kitchen`, that wakes that actor and returns its response
- `hypnos meter`, printing counts already in the ledger, with no egress line and no prices

Leave these out until an agent needs them: WebSocket streaming, the blob bucket, actor-to-actor calls, and an alarm index. A slow scan of many actor files is a signal for that index later. It is not a reason to stop using one SQLite file per actor on the wake path.
