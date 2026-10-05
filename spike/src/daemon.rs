//! Data directory for the daemon. One SQLite file per actor, a meter in
//! `system.sqlite`, and the guest wasm copied in at init.

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use anyhow::{bail, Context, Result};

use crate::host::{self, Actor, AiEntry, Meter};

const BODY_CAP: usize = 1024 * 1024;

pub fn init(root: &Path, wasm: Option<&Path>) -> Result<()> {
    let _meter = Meter::open(root)?;
    if let Some(wasm) = wasm {
        std::fs::copy(wasm, root.join("guest.wasm"))
            .with_context(|| format!("copy {}", wasm.display()))?;
    }
    Ok(())
}

pub fn meter_report(root: &Path) -> Result<String> {
    let totals = Meter::open(root)?.totals()?;
    Ok(format!(
        "requests {}\ncpu {}\ncpu_ns {}\n",
        totals.requests, totals.cpu_rows, totals.cpu_ns
    ))
}

pub async fn deploy(port: u16, name: &str, class: &str, source: &Path) -> Result<()> {
    let text =
        std::fs::read_to_string(source).with_context(|| format!("source {}", source.display()))?;
    let body = serde_json::json!({
        "name": name,
        "class": class,
        "source": text,
    })
    .to_string();
    if body.len() > BODY_CAP {
        bail!("script too large");
    }
    let url = format!("http://127.0.0.1:{port}/_hypnos/deploy");
    let client = reqwest::Client::builder().build()?;
    let resp = client
        .post(&url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .with_context(|| format!("deploy {url}"))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!("deploy failed: {status} {text}");
    }
    Ok(())
}

pub fn require_loopback(addr: SocketAddr) -> Result<()> {
    if !addr.ip().is_loopback() {
        bail!("refusing non-loopback {addr}");
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<()> {
    if name.starts_with('_') {
        bail!("bad name");
    }
    host::check_name("name", name)
}

fn validate_id(id: &str) -> Result<()> {
    if id.starts_with('_') {
        bail!("bad id");
    }
    host::check_name("id", id)
}

fn load_ai(root: &Path) -> Result<Vec<AiEntry>> {
    let path = root.join("ai.allow");
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        out.push(AiEntry::parse(line).with_context(|| format!("ai.allow line {}", i + 1))?);
    }
    Ok(out)
}

// The accept loop in the next commit is what calls this.
#[allow(dead_code)]
struct Daemon {
    root: PathBuf,
    scripts: RefCell<HashMap<String, String>>,
    actors: RefCell<HashMap<(String, String), Rc<tokio::sync::Mutex<Actor>>>>,
}

#[allow(dead_code)]
impl Daemon {
    fn slot(&self, name: &str, id: &str) -> Result<Rc<tokio::sync::Mutex<Actor>>> {
        validate_id(id)?;
        let script = self
            .scripts
            .borrow()
            .get(name)
            .cloned()
            .context("not deployed")?;
        let key = (name.to_string(), id.to_string());
        if let Some(existing) = self.actors.borrow().get(&key) {
            return Ok(existing.clone());
        }
        let actor = Actor::new(&self.root, name, id, &script)?;
        let slot = Rc::new(tokio::sync::Mutex::new(actor));
        self.actors.borrow_mut().insert(key, slot.clone());
        Ok(slot)
    }

    async fn apply_deploy(&self, name: &str, class: &str, source: &str) -> Result<()> {
        if class != "actor" {
            bail!("only class actor");
        }
        validate_name(name)?;
        if source.len() > BODY_CAP {
            bail!("script too large");
        }
        let dir = self.root.join("scripts");
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join(format!("{name}.js")), source)?;
        self.scripts
            .borrow_mut()
            .insert(name.to_string(), source.to_string());
        let slots: Vec<_> = self
            .actors
            .borrow()
            .iter()
            .filter(|((n, _), _)| n == name)
            .map(|(_, slot)| slot.clone())
            .collect();
        for slot in slots {
            let mut actor = slot.lock().await;
            actor.script = source.to_string();
            actor.sleep();
        }
        Ok(())
    }
}

fn load_scripts(root: &Path) -> Result<HashMap<String, String>> {
    let dir = root.join("scripts");
    let mut out = HashMap::new();
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e.into()),
    };
    for ent in entries {
        let path = ent?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("js") {
            continue;
        }
        let Some(name) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        validate_name(name).with_context(|| format!("script {}", path.display()))?;
        let src =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        out.insert(name.to_string(), src);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn loopback_guard() {
        let open = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8787);
        assert!(require_loopback(open).is_err());
        let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8787);
        assert!(require_loopback(local).is_ok());
    }

    #[test]
    fn meter_starts_empty() {
        let root = temp_root("meter");
        init(&root, None).unwrap();
        let text = meter_report(&root).unwrap();
        assert_eq!(text, "requests 0\ncpu 0\ncpu_ns 0\n");
        assert!(!text.contains("egress"));
        assert!(!text.contains("price"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn loads_allow_list_and_scripts() {
        let root = temp_root("load");
        init(&root, None).unwrap();
        std::fs::write(
            root.join("ai.allow"),
            "# comment\napi.openai.com=OPENAI_API_KEY\n\n",
        )
        .unwrap();
        std::fs::create_dir(root.join("scripts")).unwrap();
        std::fs::write(root.join("scripts/assistant.js"), "export default {}\n").unwrap();
        let ai = load_ai(&root).unwrap();
        assert_eq!(ai.len(), 1);
        assert_eq!(ai[0].host, "api.openai.com");
        let scripts = load_scripts(&root).unwrap();
        assert_eq!(scripts.get("assistant").map(String::as_str), Some("export default {}\n"));
        assert!(validate_name("assistant").is_ok());
        assert!(validate_name("_hypnos").is_err());
        assert!(validate_id("kitchen").is_ok());
        assert!(validate_id("_x").is_err());
        let _ = std::fs::remove_dir_all(&root);
    }

    fn temp_root(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "hypnos-daemon-{label}-{}-{nanos}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        root
    }
}
