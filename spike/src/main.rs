mod host;

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use host::{Actor, AiEntry, Eng, GuestCfg, Meter};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = args.first().map(String::as_str).unwrap_or("");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let result = rt.block_on(async {
        match cmd {
            "sysinfo" => {
                println!("{}", host::sysinfo_line());
                Ok(())
            }
            "compile" => cmd_compile(&args).await,
            "run" => cmd_run(&args).await,
            "trap" => cmd_trap(&args).await,
            "agent" => cmd_agent(&args).await,
            "wake-bench" => cmd_wake(&args).await,
            "crash" => cmd_crash(&args),
            "crash-child" => cmd_crash_child(&args).await,
            "churn" => cmd_churn(&args).await,
            "boot-scan" => cmd_boot(&args).await,
            _ => {
                usage();
                Ok(())
            }
        }
    });
    if let Err(e) = result {
        eprintln!("FAIL {e:#}");
        std::process::exit(1);
    }
}

fn usage() {
    eprintln!(
        "spike sysinfo | compile | run | trap | agent | wake-bench | crash | crash-child | churn | boot-scan"
    );
}

fn flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

fn opt(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .find(|w| w[0] == name)
        .map(|w| w[1].clone())
}

fn opt_u64(args: &[String], name: &str, default: u64) -> u64 {
    opt(args, name)
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn positionals(args: &[String]) -> Vec<String> {
    const VALUED: &[&str] = &[
        "--target",
        "-o",
        "--wasm",
        "--cwasm",
        "--script",
        "--root",
        "--iters",
        "--actors",
        "--rounds",
        "--n",
        "--kills",
        "--fail-each",
        "--ai",
        "--delay-ms",
        "--handler",
        "--cold-iters",
        "--real",
    ];
    let mut out = Vec::new();
    let mut i = 1;
    while i < args.len() {
        if VALUED.contains(&args[i].as_str()) {
            i += 2;
            continue;
        }
        if args[i].starts_with('-') {
            i += 1;
            continue;
        }
        out.push(args[i].clone());
        i += 1;
    }
    out
}

fn root_of(args: &[String]) -> PathBuf {
    opt(args, "--root")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("hypnos-data"))
}

fn must_wasm(args: &[String]) -> Result<PathBuf> {
    opt(args, "--wasm")
        .map(PathBuf::from)
        .context("--wasm is required")
}

fn load_script(args: &[String]) -> Result<String> {
    let path = opt(args, "--script").context("--script is required")?;
    Ok(std::fs::read_to_string(path)?)
}

fn eng_from(args: &[String], meter: bool, ticks: u64, ai: Vec<AiEntry>) -> Result<Eng> {
    let cfg = GuestCfg::new(ai, meter, ticks)?;
    if let Some(wasm) = opt(args, "--wasm") {
        host::open_engine(Path::new(&wasm), true, cfg)
    } else if let Some(cwasm) = opt(args, "--cwasm") {
        host::open_engine(Path::new(&cwasm), false, cfg)
    } else {
        bail!("--wasm or --cwasm is required")
    }
}

async fn cmd_compile(args: &[String]) -> Result<()> {
    let pos = positionals(args);
    let wasm = pos
        .first()
        .map(PathBuf::from)
        .context("compile needs a wasm path")?;
    let started = Instant::now();
    if flag(args, "--target") {
        let t = opt(args, "--target").context("--target needs a value")?;
        if t != "x86_64" {
            bail!("only --target x86_64");
        }
        let out = opt(args, "-o")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("engine/guest.x64.cwasm"));
        let n = host::compile_file(&wasm, &out, Some("x86_64-unknown-linux-gnu"))?;
        let host_eng = wasmtime::Engine::new(&host::engine_config(
            host::pooling_from_env(),
            true,
            None,
        )?)?;
        let bytes = std::fs::read(&out)?;
        match unsafe { wasmtime::Module::deserialize(&host_eng, &bytes) } {
            Ok(_) => bail!("x86_64 cwasm loaded on this host"),
            Err(e) => println!("x86_64 cwasm rejected on this host: {e}"),
        }
        println!(
            "compiled {} {n} bytes in {} ms (baseline x86_64, no inferred AVX)",
            out.display(),
            started.elapsed().as_millis()
        );
        return Ok(());
    }
    let out = opt(args, "-o")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("engine/guest.cwasm"));
    let n = host::compile_file(&wasm, &out, None)?;
    host::reject_foreign(&wasm)?;
    let engine = wasmtime::Engine::new(&host::engine_config(host::pooling_from_env(), true, None)?)?;
    let bytes = std::fs::read(&out)?;
    let (imports, exports) = host::describe_module(&engine, &bytes)?;
    for name in ["alloc", "init", "call", "last_error", "memory"] {
        if !exports.iter().any(|e| e == name) {
            bail!("missing export {name}");
        }
    }
    for name in ["hypnos.sql", "hypnos.set_alarm", "hypnos.delete_alarm", "hypnos.fetch"] {
        if !imports.iter().any(|e| e == name) {
            bail!("missing import {name}");
        }
    }
    println!("imports {}", imports.len());
    for i in imports.iter().filter(|i| i.starts_with("hypnos.")) {
        println!("  {i}");
    }
    println!("exports {}", exports.join(" "));
    let cfg = GuestCfg::new(Vec::new(), false, host::EPOCH_TICKS)?;
    let eng = host::open_engine(&out, false, cfg)?;
    let script = "export default { async fetch(){ return {ok:1, now: Date.now()} } }";
    let mut actor = Actor::new(Path::new("hypnos-data"), "prove", "once", script)?;
    let meter = Meter::open(Path::new("hypnos-data"))?;
    let first = actor.request(&eng, &meter, "{}").await?;
    std::thread::sleep(Duration::from_millis(5));
    actor.sleep();
    let second = actor.request(&eng, &meter, "{}").await?;
    println!("prove first {first} second {second}");
    if first.get("ok").and_then(|v| v.as_i64()) != Some(1) {
        bail!("prove script did not return ok:1");
    }
    let a = first.get("now").and_then(|v| v.as_i64()).unwrap_or(0);
    let b = second.get("now").and_then(|v| v.as_i64()).unwrap_or(0);
    if b <= a {
        bail!("Date.now did not advance ({a} then {b})");
    }
    println!(
        "compiled {} {n} bytes in {} ms",
        out.display(),
        started.elapsed().as_millis()
    );
    println!("PASS compile");
    Ok(())
}

async fn cmd_run(args: &[String]) -> Result<()> {
    let script_path = opt(args, "--script").context("--script is required")?;
    let root = root_of(args);
    let meter = Meter::open(&root)?;
    println!("pid {}", std::process::id());
    let mut script = std::fs::read_to_string(&script_path)?;
    let mut eng = eng_from(args, true, host::EPOCH_TICKS, Vec::new())?;
    let mut actor = Actor::new(&root, "run", "main", &script)?;
    let out = actor.request(&eng, &meter, "{}").await?;
    println!("{out}");
    let stdin = std::io::stdin();
    let mut line = String::new();
    let mut locked = stdin.lock();
    while locked.read_line(&mut line)? > 0 {
        script = std::fs::read_to_string(&script_path)?;
        eng = eng_from(args, true, host::EPOCH_TICKS, Vec::new())?;
        actor.script = script.clone();
        actor.sleep();
        let out = actor.request(&eng, &meter, "{}").await?;
        println!("pid {} {out}", std::process::id());
        line.clear();
    }
    println!("PASS run");
    Ok(())
}

async fn cmd_trap(args: &[String]) -> Result<()> {
    let root = root_of(args);
    let script = load_script(args)?;
    let meter = Meter::open(&root)?;
    let pid = std::process::id();
    let mut eng = eng_from(args, true, host::EPOCH_TICKS, Vec::new())?;
    let cases = [
        ("heap", r#"{"trap":"heap"}"#, 30_000u64),
        ("loop", r#"{"trap":"loop"}"#, host::EPOCH_TICKS),
        ("recurse", r#"{"trap":"recurse"}"#, host::EPOCH_TICKS),
        ("throw", r#"{"trap":"throw"}"#, host::EPOCH_TICKS),
    ];
    for (name, req, ticks) in cases {
        eng.cfg.epoch_ticks = ticks;
        let before = meter.ledger()?;
        let mut actor = Actor::new(&root, "trap", name, &script)?;
        let err = actor
            .request(&eng, &meter, req)
            .await
            .err()
            .context(format!("{name} did not fail"))?;
        let rows = host::table_count(&actor.path, "c")?;
        if rows != 0 {
            bail!("{name}: insert survived a trap (count {rows})");
        }
        let after = meter.ledger()?;
        if after.requests != before.requests + 1 || after.cpus != before.cpus + 1 {
            bail!(
                "{name}: meter requests {}->{} cpus {}->{}",
                before.requests,
                after.requests,
                before.cpus,
                after.cpus
            );
        }
        eng.cfg.epoch_ticks = host::EPOCH_TICKS;
        let ok = actor.request(&eng, &meter, "{}").await?;
        if std::process::id() != pid {
            bail!("pid changed");
        }
        println!("PASS trap {name} pid {pid} err {err:#} next {ok}");
    }
    eng.cfg.epoch_ticks = host::EPOCH_TICKS;
    let before = meter.ledger()?;
    let mut actor = Actor::new(&root, "trap", "syntax", "this is not javascript")?;
    let err = actor
        .request(&eng, &meter, "{}")
        .await
        .err()
        .context("syntax error did not fail")?;
    let after = meter.ledger()?;
    if after.requests != before.requests + 1 || after.cpus != before.cpus + 1 {
        bail!(
            "syntax: meter requests {}->{} cpus {}->{}",
            before.requests,
            after.requests,
            before.cpus,
            after.cpus
        );
    }
    actor.script = script;
    let ok = actor.request(&eng, &meter, "{}").await?;
    println!("PASS trap syntax pid {pid} err {err:#} next {ok}");
    Ok(())
}

async fn cmd_agent(args: &[String]) -> Result<()> {
    if let Some(url) = opt(args, "--real") {
        let ai = opt(args, "--ai")
            .map(|s| AiEntry::parse(&s))
            .transpose()?
            .into_iter()
            .collect();
        let eng = eng_from(args, true, host::EPOCH_TICKS, ai)?;
        let root = root_of(args);
        let meter = Meter::open(&root)?;
        let script = load_script(args)?;
        let mut actor = Actor::new(&root, "agent", "real", &script)?;
        let body = format!(r#"{{"url":{url:?},"text":"hello"}}"#);
        let out = actor.request(&eng, &meter, &body).await?;
        println!("{out}");
        return Ok(());
    }
    let delay_ms = opt_u64(args, "--delay-ms", 5000);
    let hits = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let hits2 = hits.clone();
    tokio::spawn(async move {
        mock_loop(listener, hits2, Duration::from_millis(delay_ms)).await;
    });
    let allow = format!("127.0.0.1:{}=HYPNOS_TEST_KEY", addr.port());
    let key = "hypnos-key-scan-9f3a2c";
    unsafe { std::env::set_var("HYPNOS_TEST_KEY", key) };
    let eng = eng_from(args, true, host::EPOCH_TICKS, vec![AiEntry::parse(&allow)?])?;
    let root = root_of(args);
    let meter = Meter::open(&root)?;
    let agent_src = load_script(args)?;
    let counter_src = std::fs::read_to_string("scripts/counter.js")?;
    let mut probe = Actor::new(&root, "agent", "probe", &agent_src)?;
    let wrong = addr.port() + 1;
    let probe_req = format!(
        r#"{{"probe":["http://evil.example/v1","http://127.0.0.1:{wrong}/v1"]}}"#
    );
    let probed = probe.request(&eng, &meter, &probe_req).await?;
    if hits.load(Ordering::SeqCst) != 0 {
        bail!("disallowed host reached the mock");
    }
    let outs = probed["out"]
        .as_array()
        .context("probe result")?
        .clone();
    if outs.iter().any(|v| v.as_str() == Some("allowed")) {
        bail!("a disallowed host was allowed: {probed}");
    }
    println!("PASS agent reject {probed}");

    let mut counter = Actor::new(&root, "agent", "counter", &counter_src)?;
    let mut idle = Vec::new();
    for _ in 0..200 {
        let t = Instant::now();
        counter.request(&eng, &meter, "{}").await?;
        idle.push(t.elapsed());
    }
    let idle_p99 = pct_dur(&mut idle, 0.99);

    let mut agent = Actor::new(&root, "agent", "chat", &agent_src)?;
    let url = format!("http://{addr}/v1/chat");
    let req = format!(r#"{{"url":{url:?},"text":"hello"}}"#);
    let agent_fut = agent.request(&eng, &meter, &req);
    let counter_fut = async {
        let mut lats = Vec::new();
        for _ in 0..200 {
            let t = Instant::now();
            counter.request(&eng, &meter, "{}").await?;
            lats.push(t.elapsed());
        }
        Ok::<_, anyhow::Error>(lats)
    };
    let (agent_res, lats) = tokio::join!(agent_fut, counter_fut);
    let agent_res = agent_res?;
    let mut lats = lats?;
    let live_p99 = pct_dur(&mut lats, 0.99);
    let slack = idle_p99.saturating_mul(2) + Duration::from_millis(5);
    if live_p99 > slack {
        bail!("counter p99 {live_p99:?} exceeded idle {idle_p99:?} * 2");
    }
    let cpu = meter.cpu_of("chat")?.context("missing cpu row")?;
    if cpu > 50_000_000 {
        bail!("agent cpu {cpu} ns includes the model wait");
    }
    if !agent_res["body"].as_str().unwrap_or("").contains("ok") {
        bail!("unexpected agent body {agent_res}");
    }
    if agent.memory_contains(key.as_bytes())? {
        bail!("api key is in guest memory");
    }
    println!(
        "PASS agent overlap idle_p99_us {} live_p99_us {} cpu_ns {cpu}",
        idle_p99.as_micros(),
        live_p99.as_micros()
    );

    let mut trap = Actor::new(&root, "agent", "trap", &agent_src)?;
    let trap_req = format!(r#"{{"url":{url:?},"trap_after":true}}"#);
    let err = trap.request(&eng, &meter, &trap_req).await.err().context("trap_after did not fail")?;
    let bodies = host::message_bodies(&trap.path)?;
    if !bodies.iter().any(|b| b == "kept") || bodies.iter().any(|b| b == "lost") {
        bail!("commit point failed, bodies {bodies:?} err {err:#}");
    }
    println!("PASS agent commit-point");
    Ok(())
}
