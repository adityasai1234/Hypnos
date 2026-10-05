# Hypnos

Hypnos is one Linux daemon for hosting AI agents as actors. An idle actor is a SQLite file. A request wakes it, runs its JavaScript, then drops the guest from RAM after it has been idle. The host is Rust embedding Wasmtime and QuickJS (`rquickjs`) compiled to `wasm32-wasip1`. Guests write JavaScript. Do not wrap `workerd`.

Model calls go through an allow-listed `env.AI` binding. Local and remote models are both in scope. `env.AI.fetch` is a commit point: writes made before the call stay durable if the handler traps later.

The code in this repo is the measurement spike, not the daemon. There is no `hypnos serve` and no `hypnos deploy`. Spike numbers are provisional until the same wake bench has been run on the real Linux box.

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

Copy this repo to the Linux laptop and run the spike there. That runs one bench, then exits. It does not keep an agent deployed.

```bash
bash suite.sh
```

The first product slice, after the wake bench has been repeated on that box, is:

- `hypnos init` and `hypnos serve` on `127.0.0.1`
- `hypnos deploy` for one actor script, loaded without a restart
- One path, such as `/assistant/kitchen`, that wakes that actor and returns its response
- `hypnos meter`, printing counts already in the ledger, with no egress line and no prices

Leave these out until an agent needs them: WebSocket streaming, the blob bucket, actor-to-actor calls, and an alarm index. A slow scan of many actor files is a signal for that index later. It is not a reason to stop using one SQLite file per actor on the wake path.
