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
