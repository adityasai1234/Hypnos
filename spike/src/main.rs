use spike::host;

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
    std::fs::read_to_string(&path).with_context(|| format!("script {path}"))
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
    let mut script = std::fs::read_to_string(&script_path)
        .with_context(|| format!("script {script_path}"))?;
    let mut eng = eng_from(args, true, host::EPOCH_TICKS, Vec::new())?;
    let mut actor = Actor::new(&root, "run", "main", &script)?;
    let out = actor.request(&eng, &meter, "{}").await?;
    println!("{out}");
    let stdin = std::io::stdin();
    let mut line = String::new();
    let mut locked = stdin.lock();
    while locked.read_line(&mut line)? > 0 {
        script = std::fs::read_to_string(&script_path)
            .with_context(|| format!("script {script_path}"))?;
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

async fn mock_loop(listener: TcpListener, hits: Arc<AtomicUsize>, delay: Duration) {
    loop {
        let Ok((mut sock, _)) = listener.accept().await else {
            break;
        };
        hits.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            let _ = read_http(&mut sock).await;
            tokio::time::sleep(delay).await;
            let body = br#"{"content":"ok"}"#;
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = sock.write_all(head.as_bytes()).await;
            let _ = sock.write_all(body).await;
        });
    }
}

async fn read_http(sock: &mut tokio::net::TcpStream) -> Result<()> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        let n = sock.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let header = String::from_utf8_lossy(&buf[..i]);
            let len = header.lines().find_map(|line| {
                let (k, v) = line.split_once(':')?;
                if k.eq_ignore_ascii_case("content-length") {
                    v.trim().parse::<usize>().ok()
                } else {
                    None
                }
            });
            if let Some(len) = len {
                while buf.len() < i + 4 + len {
                    let n = sock.read(&mut tmp).await?;
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
            }
            break;
        }
    }
    Ok(())
}

async fn cmd_wake(args: &[String]) -> Result<()> {
    let iters = opt_u64(args, "--iters", 1000);
    let cold_iters = opt_u64(args, "--cold-iters", iters.min(30));
    let meter_on = !flag(args, "--no-meter");
    let handler = opt(args, "--handler").unwrap_or_else(|| "both".into());
    let root = root_of(args);
    let script = load_script(args)?;
    let eng = eng_from(args, meter_on, host::EPOCH_TICKS, Vec::new())?;
    let meter = Meter::open(&root)?;
    heap_check(&eng, &meter, &root, &script).await?;

    let handlers: &[&str] = match handler.as_str() {
        "read" => &["read"],
        "write" => &["write"],
        _ => &["read", "write"],
    };
    for kind in handlers {
        let req = if *kind == "read" {
            r#"{"op":"read"}"#
        } else {
            "{}"
        };
        let hot = bench_class(&eng, &meter, &root, &script, kind, req, iters, "hot", false).await?;
        let warm = bench_class(&eng, &meter, &root, &script, kind, req, iters, "warm", false).await?;
        match host::drop_page_cache() {
            Ok(()) => {
                let cold = bench_class(
                    &eng, &meter, &root, &script, kind, req, cold_iters, "cold", true,
                )
                .await?;
                print_samples(kind, meter_on, "cold", &cold);
            }
            Err(e) => println!("SKIP cold {kind}: {e:#}"),
        }
        print_samples(kind, meter_on, "hot", &hot);
        print_samples(kind, meter_on, "warm", &warm);
        if *kind == "write" && meter_on {
            let mut totals: Vec<u128> = warm.iter().map(|s| s.total).collect();
            let p50 = pct(&mut totals, 0.50);
            let engine: u128 = warm.iter().map(|s| s.instantiate + s.eval).sum::<u128>()
                / warm.len().max(1) as u128;
            let verdict = if p50 < 10_000_000 && engine * 2 < p50.max(1) {
                "INSIDE"
            } else if engine > 20_000_000 {
                "NO-GO"
            } else {
                "OUTSIDE"
            };
            println!(
                "fixed-line warm-write p50_us {} engine_us {} {verdict}",
                p50 / 1000,
                engine / 1000
            );
        }
    }
    println!("PASS wake-bench");
    Ok(())
}

async fn heap_check(eng: &Eng, meter: &Meter, root: &Path, script: &str) -> Result<()> {
    let mut actor = Actor::new(root, "bench", "heap", script)?;
    for i in 1..=3 {
        let v = actor.request(eng, meter, "{}").await?;
        let heap = v["heap"].as_i64().unwrap_or(0);
        let sql = v["sql"].as_i64().unwrap_or(0);
        if heap != i || sql != i {
            bail!("warm heap check got {v}, want heap {i} sql {i}");
        }
    }
    if host::live_count() == 0 {
        bail!("actor was not live");
    }
    actor.sleep();
    if host::live_count() != 0 {
        bail!("heap still live after sleep, count {}", host::live_count());
    }
    let v = actor.request(eng, meter, "{}").await?;
    if v["heap"].as_i64() != Some(1) || v["sql"].as_i64() != Some(4) {
        bail!("wake after sleep got {v}, want heap 1 sql 4");
    }
    actor.request(eng, meter, "{}").await?;
    std::thread::sleep(Duration::from_millis(60));
    let mut pair = [actor];
    host::sweep(&mut pair, Instant::now(), Duration::from_millis(50));
    if pair[0].is_awake() || host::live_count() != 0 {
        bail!("sweep did not drop the idle actor");
    }
    println!("PASS heap-reset");
    Ok(())
}

struct Sample {
    open: u128,
    instantiate: u128,
    eval: u128,
    handler: u128,
    total: u128,
}

async fn bench_class(
    eng: &Eng,
    meter: &Meter,
    root: &Path,
    script: &str,
    kind: &str,
    req: &str,
    iters: u64,
    class: &str,
    cold: bool,
) -> Result<Vec<Sample>> {
    let mut actor = Actor::new(root, "bench", &format!("{kind}-{class}"), script)?;
    actor.request(eng, meter, "{}").await?;
    if class != "hot" {
        actor.sleep();
    }
    let mut out = Vec::with_capacity(iters as usize);
    for i in 0..iters {
        if class != "hot" {
            actor.sleep();
        }
        if cold {
            host::drop_page_cache()?;
        }
        let t = Instant::now();
        actor.request(eng, meter, req).await?;
        let total = t.elapsed().as_nanos();
        out.push(Sample {
            open: actor.phases.open_sqlite.as_nanos(),
            instantiate: actor.phases.instantiate.as_nanos(),
            eval: actor.phases.eval.as_nanos(),
            handler: actor.phases.handler.as_nanos(),
            total,
        });
        if i > 0 && i % 100 == 0 {
            println!("{class} {kind} {i}/{iters}");
        }
    }
    Ok(out)
}

fn print_samples(kind: &str, meter_on: bool, class: &str, samples: &[Sample]) {
    let mut open: Vec<u128> = samples.iter().map(|s| s.open).collect();
    let mut inst: Vec<u128> = samples.iter().map(|s| s.instantiate).collect();
    let mut eval: Vec<u128> = samples.iter().map(|s| s.eval).collect();
    let mut handler: Vec<u128> = samples.iter().map(|s| s.handler).collect();
    let mut total: Vec<u128> = samples.iter().map(|s| s.total).collect();
    let meter = if meter_on { "meter" } else { "no-meter" };
    println!(
        "{class} {kind} {meter} n {} open_us {}/{}/{} instantiate_us {}/{}/{} eval_us {}/{}/{} handler_us {}/{}/{} total_us {}/{}/{}",
        samples.len(),
        pct(&mut open, 0.50) / 1000,
        pct(&mut open, 0.95) / 1000,
        pct(&mut open, 0.99) / 1000,
        pct(&mut inst, 0.50) / 1000,
        pct(&mut inst, 0.95) / 1000,
        pct(&mut inst, 0.99) / 1000,
        pct(&mut eval, 0.50) / 1000,
        pct(&mut eval, 0.95) / 1000,
        pct(&mut eval, 0.99) / 1000,
        pct(&mut handler, 0.50) / 1000,
        pct(&mut handler, 0.95) / 1000,
        pct(&mut handler, 0.99) / 1000,
        pct(&mut total, 0.50) / 1000,
        pct(&mut total, 0.95) / 1000,
        pct(&mut total, 0.99) / 1000,
    );
}

fn pct(xs: &mut [u128], p: f64) -> u128 {
    if xs.is_empty() {
        return 0;
    }
    xs.sort_unstable();
    let i = ((xs.len() - 1) as f64 * p).round() as usize;
    xs[i.min(xs.len() - 1)]
}

fn pct_dur(xs: &mut [Duration], p: f64) -> Duration {
    let mut ns: Vec<u128> = xs.iter().map(|d| d.as_nanos()).collect();
    Duration::from_nanos(pct(&mut ns, p) as u64)
}

fn cmd_crash(args: &[String]) -> Result<()> {
    let wasm = must_wasm(args)?;
    let script = opt(args, "--script").context("--script")?;
    let cwasm = PathBuf::from("engine/guest.cwasm");
    host::compile_file(&wasm, &cwasm, None)?;
    let kills = opt_u64(args, "--kills", 500);
    let each = opt_u64(args, "--fail-each", 100);
    let base = root_of(args).join("crash");
    let _ = std::fs::remove_dir_all(&base);
    for point in ["after_request", "inside_guest", "after_guest", "after_commit"] {
        for i in 0..each {
            let dir = base.join(point).join(i.to_string());
            std::fs::create_dir_all(&dir)?;
            let mut child = spawn_child(&dir, &cwasm, Path::new(&script), Some(point))?;
            let mut stdout = child.stdout.take().unwrap();
            let reader = std::thread::spawn(move || {
                let mut s = String::new();
                let _ = stdout.read_to_string(&mut s);
                s
            });
            let started = Instant::now();
            loop {
                if child.try_wait()?.is_some() {
                    break;
                }
                if started.elapsed() > Duration::from_secs(30) {
                    let _ = child.kill();
                    bail!("{point} #{i} did not abort");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let out = reader.join().unwrap_or_default();
            let actor = host::actor_path(&dir, "crash", "one");
            host::check_files(&dir, &actor)?;
            check_acks(&out, &actor)?;
            if i == 0 || i + 1 == each {
                println!("PASS failpoint {point} #{i}");
            }
        }
        println!("PASS failpoint {point} x {each}");
    }
    for i in 0..kills {
        let dir = base.join("random").join(i.to_string());
        std::fs::create_dir_all(&dir)?;
        let mut child = spawn_child(&dir, &cwasm, Path::new(&script), None)?;
        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 || !line.starts_with("ready") {
            let _ = child.kill();
            bail!("random #{i} never ready: {line}");
        }
        let delay = i.wrapping_mul(1103515245).wrapping_add(12345) % 101;
        std::thread::sleep(Duration::from_millis(delay as u64));
        let _ = child.kill();
        let _ = child.wait();
        let mut rest = String::new();
        reader.read_to_string(&mut rest)?;
        let actor = host::actor_path(&dir, "crash", "one");
        host::check_files(&dir, &actor)?;
        check_acks(&rest, &actor)?;
        if i == 0 || i + 1 == kills || (i + 1) % 50 == 0 {
            println!("PASS random #{i} delay {delay} ms");
        }
    }
    println!("PASS crash");
    Ok(())
}

fn spawn_child(root: &Path, cwasm: &Path, script: &Path, fail: Option<&str>) -> Result<std::process::Child> {
    let mut cmd = Command::new(std::env::current_exe()?);
    cmd.arg("crash-child")
        .arg("--root")
        .arg(root)
        .arg("--cwasm")
        .arg(cwasm)
        .arg("--script")
        .arg(script)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .env_remove("HYPNOS_FAILPOINT");
    if let Some(point) = fail {
        cmd.env("HYPNOS_FAILPOINT", point);
    }
    Ok(cmd.spawn()?)
}

fn check_acks(out: &str, actor_db: &Path) -> Result<()> {
    let mut last = None;
    for line in out.lines() {
        if let Some(n) = line.strip_prefix("ack ") {
            last = Some(n.trim().parse::<i64>().context("ack")?);
        }
    }
    let count = host::table_count(actor_db, "c")?;
    if let Some(ack) = last {
        if count < ack || count > ack + 1 {
            bail!("acked {ack} but c has {count}");
        }
    }
    Ok(())
}

async fn cmd_crash_child(args: &[String]) -> Result<()> {
    let root = root_of(args);
    let script = load_script(args)?;
    let eng = eng_from(args, true, host::EPOCH_TICKS, Vec::new())?;
    let meter = Meter::open(&root)?;
    let mut actor = Actor::new(&root, "crash", "one", &script)?;
    actor.wake(&eng).await?;
    println!("ready");
    let _ = std::io::stdout().flush();
    loop {
        let v = actor
            .request(&eng, &meter, r#"{"burn_ms":20}"#)
            .await?;
        println!("ack {}", v["sql"].as_i64().unwrap_or(0));
        let _ = std::io::stdout().flush();
    }
}

async fn cmd_churn(args: &[String]) -> Result<()> {
    if std::env::var("HYPNOS_CHURN_INNER").ok().as_deref() != Some("1") {
        for pooling in ["0", "1"] {
            let status = Command::new(std::env::current_exe()?)
                .args(std::env::args().skip(1))
                .env("HYPNOS_POOLING", pooling)
                .env("HYPNOS_CHURN_INNER", "1")
                .status()?;
            if !status.success() {
                bail!("churn pooling={pooling} failed");
            }
        }
        println!("PASS churn");
        return Ok(());
    }
    let actors_n = opt_u64(args, "--actors", 5000);
    let rounds = opt_u64(args, "--rounds", 5);
    let root = root_of(args);
    let script = load_script(args)?;
    let eng = eng_from(args, false, host::EPOCH_TICKS, Vec::new())?;
    let meter = Meter::open(&root)?;
    let mut base = None;
    for round in 0..rounds {
        for i in 0..actors_n {
            let mut actor = Actor::new(&root, "churn", &format!("a{i}"), &script)?;
            actor.request(&eng, &meter, "{}").await?;
            actor.sleep();
        }
        host::trim_heap();
        let rss = host::anon_rss_kb().or_else(host::vm_rss_kb).unwrap_or(0);
        let delta = base.map(|p: u64| rss as i64 - p as i64).unwrap_or(0);
        println!(
            "round {round} pooling {} rss_kb {rss} delta_kb {delta}",
            host::pooling_from_env() as u8
        );
        if round == 1 {
            base = Some(rss);
        }
        if round >= 2 {
            if let Some(start) = base {
                let grown = rss.saturating_sub(start);
                let allowed = 50 * (actors_n / 1000).max(1);
                if grown > allowed {
                    bail!("rss grew {grown} kb after round {round}, allowed {allowed}");
                }
            }
        }
    }
    Ok(())
}

async fn cmd_boot(args: &[String]) -> Result<()> {
    let spec = opt(args, "--n").unwrap_or_else(|| "1000,10000,100000".into());
    let root = root_of(args).join("scan");
    for part in spec.split(',') {
        let n: u64 = part.trim().parse().context("--n")?;
        println!("creating {n} actor files");
        host::ensure_scan_files(&root, n)?;
        let (warm_us, alarms, earliest) = host::boot_scan(&root, n).await?;
        let per = warm_us as f64 / n as f64;
        println!(
            "warm n {n} total_ms {} us_per_file {per:.1} with_alarm {alarms} earliest {earliest:?}",
            warm_us / 1000
        );
        let mut slowest = warm_us;
        match host::drop_page_cache() {
            Ok(()) => {
                let (cold_us, alarms, earliest) = host::boot_scan(&root, n).await?;
                let per = cold_us as f64 / n as f64;
                println!(
                    "cold n {n} total_ms {} us_per_file {per:.1} with_alarm {alarms} earliest {earliest:?}",
                    cold_us / 1000
                );
                slowest = slowest.max(cold_us);
            }
            Err(e) => println!("SKIP cold scan {n}: {e:#}"),
        }
        if n >= 100_000 && slowest > 10_000_000 {
            println!(
                "DESIGN SIGNAL: {n} actor files took {} ms to rearm. An alarm index in system.sqlite, rebuilt when missing, is the fallback. Not a spike fix.",
                slowest / 1000
            );
        }
    }
    println!("PASS boot-scan");
    Ok(())
}
