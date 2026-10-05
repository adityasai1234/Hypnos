use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use spike::daemon;

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = if args.is_empty() {
        String::new()
    } else {
        args.remove(0)
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let result = rt.block_on(async {
        match cmd.as_str() {
            "init" => cmd_init(&args),
            "serve" => {
                let root = root_of(&args)?;
                let port = opt_u16(&args, "--port", daemon::DEFAULT_PORT)?;
                let local = tokio::task::LocalSet::new();
                local.run_until(daemon::serve(root, port, None)).await
            }
            "deploy" => cmd_deploy(&args).await,
            "meter" => {
                let text = daemon::meter_report(&root_of(&args)?)?;
                print!("{text}");
                Ok(())
            }
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
        "hypnos init <root> [--wasm PATH] | serve <root> [--port N] | deploy --name NAME --class actor --source PATH [--port N] | meter <root>"
    );
}

fn cmd_init(args: &[String]) -> Result<()> {
    let root = root_of(args)?;
    let wasm = opt(args, "--wasm")?.map(PathBuf::from);
    daemon::init(&root, wasm.as_deref())
}

async fn cmd_deploy(args: &[String]) -> Result<()> {
    let name = opt(args, "--name")?.context("--name is required")?;
    let class = opt(args, "--class")?.context("--class is required")?;
    let source = opt(args, "--source")?.context("--source is required")?;
    let port = opt_u16(args, "--port", daemon::DEFAULT_PORT)?;
    daemon::deploy(port, &name, &class, std::path::Path::new(&source)).await
}

fn root_of(args: &[String]) -> Result<PathBuf> {
    positional(args)
        .map(PathBuf::from)
        .context("root path is required")
}

fn positional(args: &[String]) -> Option<String> {
    const VALUED: &[&str] = &["--wasm", "--port", "--name", "--class", "--source"];
    let mut i = 0;
    while i < args.len() {
        if VALUED.contains(&args[i].as_str()) {
            i += 2;
            continue;
        }
        if args[i].starts_with('-') {
            i += 1;
            continue;
        }
        return Some(args[i].clone());
    }
    None
}

fn opt(args: &[String], name: &str) -> Result<Option<String>> {
    let mut found = None;
    let mut i = 0;
    while i < args.len() {
        if args[i] == name {
            let v = args.get(i + 1).context(format!("{name} needs a value"))?;
            if v.starts_with('-') {
                bail!("{name} needs a value");
            }
            found = Some(v.clone());
            i += 2;
            continue;
        }
        i += 1;
    }
    Ok(found)
}

fn opt_u16(args: &[String], name: &str, default: u16) -> Result<u16> {
    match opt(args, name)? {
        Some(s) => s.parse().with_context(|| format!("bad {name}")),
        None => Ok(default),
    }
}
