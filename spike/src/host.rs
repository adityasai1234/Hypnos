//! Host for one QuickJS-on-Wasmtime guest per actor.
//!
//! A request is one actor transaction. `hypnos.fetch` commits that transaction
//! before it suspends and opens a new one when the model call returns, so a
//! later trap rolls back only the post-call writes.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use rusqlite::{params_from_iter, types::ToSql, Connection, OptionalExtension};
use wasmtime::{
    AsContextMut, CallHook, Caller, Config, Engine, Extern, InstancePre, Linker, Memory, Module,
    PoolingAllocationConfig, Store, StoreLimits, StoreLimitsBuilder, TypedFunc,
};
use wasmtime_wasi::p1::{self, WasiP1Ctx};
use wasmtime_wasi::WasiCtxBuilder;

pub const WASMTIME_VERSION: &str = "49.0.2";
const MEM_LIMIT: usize = 64 * 1024 * 1024;
pub const EPOCH_TICKS: u64 = 50;

static LIVE: AtomicUsize = AtomicUsize::new(0);

pub fn live_count() -> usize {
    LIVE.load(Ordering::SeqCst)
}

pub fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts);
    }
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

pub fn anon_rss_kb() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/smaps_rollup").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("Anonymous:") {
            return rest.split_whitespace().next()?.parse().ok();
        }
    }
    None
}

pub fn vm_rss_kb() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.split_whitespace().next()?.parse().ok();
        }
    }
    None
}

pub fn trim_heap() {
    #[cfg(target_env = "gnu")]
    unsafe {
        libc::malloc_trim(0);
    }
}

pub fn drop_page_cache() -> Result<()> {
    unsafe { libc::sync() };
    std::fs::write("/proc/sys/vm/drop_caches", "3")
        .context("drop_caches needs a privileged container")?;
    Ok(())
}

#[derive(Clone, Debug)]
pub struct AiEntry {
    pub host: String,
    pub port: Option<u16>,
    pub key_env: Option<String>,
}

impl AiEntry {
    pub fn parse(s: &str) -> Result<Self> {
        let (spec, key_env) = match s.split_once('=') {
            Some((h, k)) if !k.is_empty() => (h, Some(k.to_string())),
            _ => (s, None),
        };
        if let Some((h, p)) = spec.rsplit_once(':') {
            if let Ok(port) = p.parse::<u16>() {
                if h.is_empty() {
                    bail!("empty host in {s}");
                }
                return Ok(Self {
                    host: h.to_string(),
                    port: Some(port),
                    key_env,
                });
            }
        }
        if spec.is_empty() {
            bail!("empty ai entry");
        }
        Ok(Self {
            host: spec.to_string(),
            port: None,
            key_env,
        })
    }
}

#[derive(Clone)]
pub struct GuestCfg {
    pub http: reqwest::Client,
    pub ai: Arc<Vec<AiEntry>>,
    pub epoch_ticks: u64,
    pub meter: bool,
    pub failpoint: Option<String>,
}

impl GuestCfg {
    pub fn new(ai: Vec<AiEntry>, meter: bool, epoch_ticks: u64) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder().build()?,
            ai: Arc::new(ai),
            epoch_ticks,
            meter,
            failpoint: std::env::var("HYPNOS_FAILPOINT").ok(),
        })
    }
}

pub struct HostState {
    wasi: WasiP1Ctx,
    db: Connection,
    limits: StoreLimits,
    http: reqwest::Client,
    ai: Arc<Vec<AiEntry>>,
    epoch_ticks: u64,
    meter: bool,
    failpoint: Option<String>,
    cpu_ns: u64,
    cpu_mark: u64,
    in_tx: bool,
}

impl HostState {
    fn begin_tx(&mut self) -> Result<()> {
        if !self.in_tx {
            self.db.execute_batch("BEGIN IMMEDIATE")?;
            self.in_tx = true;
        }
        Ok(())
    }

    fn commit_tx(&mut self) -> Result<()> {
        if self.in_tx {
            self.db.execute_batch("COMMIT")?;
            self.in_tx = false;
        }
        Ok(())
    }

    fn rollback_tx(&mut self) -> Result<()> {
        if self.in_tx {
            self.db.execute_batch("ROLLBACK")?;
            self.in_tx = false;
        }
        Ok(())
    }
}

pub struct Eng {
    pub engine: Engine,
    pub pre: InstancePre<HostState>,
    pub cfg: GuestCfg,
}

#[derive(Clone, Copy, Default, Debug)]
pub struct Phases {
    pub open_sqlite: Duration,
    pub instantiate: Duration,
    pub eval: Duration,
    pub handler: Duration,
}

struct Live {
    store: Store<HostState>,
    memory: Memory,
    alloc: TypedFunc<i32, i32>,
    init: TypedFunc<(i32, i32), i32>,
    call: TypedFunc<(i32, i32), i64>,
    last_error: TypedFunc<(), i64>,
}

impl Drop for Live {
    fn drop(&mut self) {
        LIVE.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct Actor {
    pub ns: String,
    pub id: String,
    pub path: PathBuf,
    pub script: String,
    pub phases: Phases,
    pub last_cpu: u64,
    pub last_used: Instant,
    live: Option<Live>,
}

pub struct Meter {
    conn: Mutex<Connection>,
}

#[derive(Clone, Copy, Debug)]
pub struct Ledger {
    pub requests: i64,
    pub cpus: i64,
}

pub fn engine_config(pooling: bool, epoch: bool, target: Option<&str>) -> Result<Config> {
    let mut config = Config::new();
    config.epoch_interruption(epoch);
    config.memory_init_cow(true);
    // ponytail: no Wizer snapshot of the runtime and shim. Add one only if
    // instantiate+eval is the phase that misses the millisecond budget.
    if let Some(t) = target {
        config.target(t)?;
    }
    if pooling {
        let mut pool = PoolingAllocationConfig::default();
        // ponytail: 8 slots. The spike keeps one or two guests live. Raise this
        // with max_live when more than a handful stay resident.
        pool.total_memories(8);
        pool.total_tables(8);
        pool.max_memory_size(MEM_LIMIT);
        config.allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(pool));
    }
    Ok(config)
}

pub fn pooling_from_env() -> bool {
    std::env::var("HYPNOS_POOLING").ok().as_deref() == Some("1")
}

fn arm_epoch(engine: &Engine) {
    static ONCE: Once = Once::new();
    let engine = engine.clone();
    ONCE.call_once(|| {
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(1));
            engine.increment_epoch();
        });
    });
}

pub fn open_engine(wasm_or_cwasm: &Path, from_wasm: bool, cfg: GuestCfg) -> Result<Eng> {
    let pooling = pooling_from_env();
    let engine = Engine::new(&engine_config(pooling, true, None)?)?;
    arm_epoch(&engine);
    let cwasm_bytes;
    let module = if from_wasm {
        let wasm = std::fs::read(wasm_or_cwasm)?;
        cwasm_bytes = engine.precompile_module(&wasm)?;
        unsafe { Module::deserialize(&engine, &cwasm_bytes)? }
    } else {
        unsafe { Module::deserialize_file(&engine, wasm_or_cwasm)? }
    };
    let pre = link(&engine, &module)?;
    Ok(Eng { engine, pre, cfg })
}

pub fn compile_file(wasm_path: &Path, out: &Path, target: Option<&str>) -> Result<usize> {
    let pooling = pooling_from_env();
    let engine = Engine::new(&engine_config(pooling, true, target)?)?;
    let wasm = std::fs::read(wasm_path)?;
    let bytes = engine.precompile_module(&wasm)?;
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, &bytes)?;
    Ok(bytes.len())
}

pub fn describe_module(engine: &Engine, cwasm: &[u8]) -> Result<(Vec<String>, Vec<String>)> {
    let module = unsafe { Module::deserialize(engine, cwasm)? };
    let imports = module
        .imports()
        .map(|i| format!("{}.{}", i.module(), i.name()))
        .collect();
    let exports = module.exports().map(|e| e.name().to_string()).collect();
    Ok((imports, exports))
}

pub fn reject_foreign(wasm_path: &Path) -> Result<()> {
    let wasm = std::fs::read(wasm_path)?;
    let host = Engine::new(&engine_config(pooling_from_env(), true, None)?)?;
    let mut alt_cfg = engine_config(pooling_from_env(), false, None)?;
    let _ = alt_cfg.epoch_interruption(false);
    let alt = Engine::new(&alt_cfg)?;
    let bytes = alt.precompile_module(&wasm)?;
    match unsafe { Module::deserialize(&host, &bytes) } {
        Ok(_) => bail!("cwasm compiled with a different config was accepted"),
        Err(_) => Ok(()),
    }
}

fn link(engine: &Engine, module: &Module) -> Result<InstancePre<HostState>> {
    let mut linker = Linker::new(engine);
    p1::add_to_linker_async(&mut linker, |s: &mut HostState| &mut s.wasi)?;
    define_imports(&mut linker)?;
    Ok(linker.instantiate_pre(module)?)
}

fn to_trap(e: anyhow::Error) -> wasmtime::Error {
    wasmtime::Error::msg(e.to_string())
}

fn define_imports(linker: &mut Linker<HostState>) -> Result<()> {
    linker.func_wrap_async(
        "hypnos",
        "sql",
        |mut caller: Caller<'_, HostState>,
         (q_ptr, q_len, p_ptr, p_len): (i32, i32, i32, i32)| {
            Box::new(async move {
                let result: anyhow::Result<i64> = async move {
                    if caller.data().failpoint.as_deref() == Some("inside_guest") {
                        std::process::abort();
                    }
                    let q = read_mem(&mut caller, q_ptr, q_len)?;
                    let p = read_mem(&mut caller, p_ptr, p_len)?;
                    let out = {
                        let db = &caller.data().db;
                        run_sql(db, &q, &p)
                    };
                    write_out(&mut caller, out.as_bytes()).await
                }
                .await;
                result.map_err(to_trap)
            })
        },
    )?;
    linker.func_wrap_async(
        "hypnos",
        "set_alarm",
        |mut caller: Caller<'_, HostState>, (at_ms,): (f64,)| {
            Box::new(async move {
                let result: anyhow::Result<()> = async move {
                    caller.data_mut().db.execute(
                        "INSERT INTO _alarm(id, at_ms) VALUES(1, ?1)
                         ON CONFLICT(id) DO UPDATE SET at_ms = excluded.at_ms",
                        [at_ms as i64],
                    )?;
                    Ok(())
                }
                .await;
                result.map_err(to_trap)
            })
        },
    )?;
    linker.func_wrap_async(
        "hypnos",
        "delete_alarm",
        |mut caller: Caller<'_, HostState>, (): ()| {
            Box::new(async move {
                let result: anyhow::Result<()> = async move {
                    caller
                        .data_mut()
                        .db
                        .execute("DELETE FROM _alarm WHERE id = 1", [])?;
                    Ok(())
                }
                .await;
                result.map_err(to_trap)
            })
        },
    )?;
    linker.func_wrap_async(
        "hypnos",
        "fetch",
        |mut caller: Caller<'_, HostState>, (ptr, len): (i32, i32)| {
            Box::new(async move {
                let result: anyhow::Result<i64> = async move {
                    let raw = read_mem(&mut caller, ptr, len)?;
                    if let Err(e) = caller.data_mut().commit_tx() {
                        return write_out(&mut caller, err_json(&e.to_string()).as_bytes()).await;
                    }
                    let http = caller.data().http.clone();
                    let ai = caller.data().ai.clone();
                    let resp = do_fetch(&http, &ai, &raw).await;
                    // ponytail: no cap on concurrent model calls. Add a semaphore when more than one agent waits.
                    // ponytail: no token metering. Count usage from the response body when a model returns it.
                    // ponytail: no WebSocket streaming. The agent returns one body until a client needs live tokens.
                    caller.data_mut().begin_tx()?;
                    write_out(&mut caller, resp.as_bytes()).await
                }
                .await;
                result.map_err(to_trap)
            })
        },
    )?;
    Ok(())
}
