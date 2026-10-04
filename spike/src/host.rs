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
