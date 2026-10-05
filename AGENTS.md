## Learned User Preferences

- The main product goal is hosting AI agents as actors. A guest that cannot call a model does not meet that goal.

## Learned Workspace Facts

- Hypnos is one local Linux daemon: untrusted JavaScript, one SQLite file per actor, guests dropped from RAM when idle, a meter shaped like Workers plus R2, and response egress unpriced.
- Guests write JavaScript. The host is Rust embedding Wasmtime and QuickJS (`rquickjs`) compiled to `wasm32-wasip1`. Do not wrap `workerd`.
- The operator deploys from a remote PC over SSH. The daemon listens on loopback only. Do not bind it to `0.0.0.0`.
- Model calls are an allow-listed `env.AI` binding. Local and remote models are both in scope. `env.AI.fetch` is a commit point.
- Spike results are provisional until measured on the real Linux box. A slow boot scan of many actor files is a design signal for a later alarm index, not a reason to abandon one SQLite file per actor on the wake path.
