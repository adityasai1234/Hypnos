//! Loopback daemon. One process, one guest engine, one SQLite file per actor.
//! `serve` runs on a current-thread `LocalSet` so a model call can yield while
//! another actor runs.

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::host::{self, Actor, AiEntry, Eng, Meter};

pub const DEFAULT_PORT: u16 = 8787;
const BIND: Ipv4Addr = Ipv4Addr::LOCALHOST;

// ponytail: drop the guest after 1s idle. Lengthen this if a chat pays a full
// wake on every token turn.
const IDLE: Duration = Duration::from_secs(1);
const SWEEP: Duration = Duration::from_secs(1);
const HEADER_CAP: usize = 16 * 1024;
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

struct Daemon {
    root: PathBuf,
    eng: Eng,
    meter: Meter,
    scripts: RefCell<HashMap<String, String>>,
    actors: RefCell<HashMap<(String, String), Rc<tokio::sync::Mutex<Actor>>>>,
}

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

/// Caller must be inside a [`tokio::task::LocalSet`].
pub async fn serve(
    root: PathBuf,
    port: u16,
    ready: Option<tokio::sync::oneshot::Sender<SocketAddr>>,
) -> Result<()> {
    let wasm = root.join("guest.wasm");
    if !wasm.is_file() {
        bail!(
            "guest.wasm is missing in {}; run hypnos init <root> --wasm <hypnos_guest.wasm>",
            root.display()
        );
    }
    let listener = TcpListener::bind((BIND, port)).await?;
    let addr = listener.local_addr()?;
    require_loopback(addr)?;
    let ai = load_ai(&root)?;
    let cfg = host::GuestCfg::new(ai, true, host::EPOCH_TICKS)?;
    let eng = host::open_engine(&wasm, true, cfg)?;
    let meter = Meter::open(&root)?;
    let daemon = Rc::new(Daemon {
        root: root.clone(),
        eng,
        meter,
        scripts: RefCell::new(load_scripts(&root)?),
        actors: RefCell::new(HashMap::new()),
    });
    let sweep = daemon.clone();
    tokio::task::spawn_local(async move { sweep_idle(sweep).await });
    println!("listening http://{addr}");
    if let Some(tx) = ready {
        let _ = tx.send(addr);
    }
    loop {
        let (sock, _) = listener.accept().await?;
        let daemon = daemon.clone();
        tokio::task::spawn_local(async move {
            if let Err(e) = talk(sock, daemon).await {
                eprintln!("FAIL {e:#}");
            }
        });
    }
}

async fn sweep_idle(daemon: Rc<Daemon>) {
    loop {
        tokio::time::sleep(SWEEP).await;
        let slots: Vec<_> = daemon.actors.borrow().values().cloned().collect();
        for slot in slots {
            let mut actor = slot.lock().await;
            if actor.is_awake() && actor.last_used.elapsed() >= IDLE {
                actor.sleep();
            }
        }
    }
}

struct Reply {
    status: u16,
    content_type: &'static str,
    body: String,
}

impl Reply {
    fn text(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8",
            body: body.into(),
        }
    }

    fn json(status: u16, value: &serde_json::Value) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: format!("{value}\n"),
        }
    }

    fn reason(&self) -> &'static str {
        match self.status {
            200 => "OK",
            400 => "Bad Request",
            404 => "Not Found",
            405 => "Method Not Allowed",
            500 => "Internal Server Error",
            _ => "Error",
        }
    }
}

enum Route {
    Deploy,
    Actor(String, String),
    NotFound,
}

fn route(method: &str, path: &str) -> Result<Route> {
    if path == "/_hypnos/deploy" {
        if method != "POST" {
            bail!("POST only");
        }
        return Ok(Route::Deploy);
    }
    if method != "GET" && method != "POST" {
        bail!("GET or POST");
    }
    let mut segs = path.split('/').filter(|seg| !seg.is_empty());
    let Some(name) = segs.next() else {
        return Ok(Route::NotFound);
    };
    let Some(id) = segs.next() else {
        return Ok(Route::NotFound);
    };
    if segs.next().is_some() {
        bail!("bad path");
    }
    validate_name(name)?;
    validate_id(id)?;
    Ok(Route::Actor(name.to_string(), id.to_string()))
}

async fn talk(mut sock: TcpStream, daemon: Rc<Daemon>) -> Result<()> {
    let req = match read_request(&mut sock).await {
        Ok(req) => req,
        Err(e) => {
            let reply = Reply::text(400, format!("{e:#}"));
            write_reply(&mut sock, &reply).await?;
            return Ok(());
        }
    };
    let reply = dispatch(&daemon, req).await;
    write_reply(&mut sock, &reply).await
}

async fn dispatch(daemon: &Daemon, req: HttpReq) -> Reply {
    let route = match route(&req.method, &req.path) {
        Ok(route) => route,
        Err(e) => return Reply::text(400, format!("{e:#}")),
    };
    match route {
        Route::NotFound => Reply::text(404, "not found"),
        Route::Deploy => match deploy_body(daemon, &req.body).await {
            Ok(()) => Reply::json(200, &serde_json::json!({ "ok": true })),
            Err(e) => Reply::text(400, format!("{e:#}")),
        },
        Route::Actor(name, id) => actor_request(daemon, &name, &id, &req.body).await,
    }
}

async fn deploy_body(daemon: &Daemon, body: &[u8]) -> Result<()> {
    let value: serde_json::Value =
        serde_json::from_slice(body).context("deploy body is not json")?;
    let name = value.get("name").and_then(|v| v.as_str()).context("name")?;
    let class = value
        .get("class")
        .and_then(|v| v.as_str())
        .context("class")?;
    let source = value
        .get("source")
        .and_then(|v| v.as_str())
        .context("source")?;
    daemon.apply_deploy(name, class, source).await
}

async fn actor_request(daemon: &Daemon, name: &str, id: &str, body: &[u8]) -> Reply {
    if !daemon.scripts.borrow().contains_key(name) {
        return Reply::text(404, "not deployed");
    }
    let text = if body.is_empty() {
        "{}".to_string()
    } else {
        match std::str::from_utf8(body) {
            Ok(text) => text.to_string(),
            Err(_) => return Reply::text(400, "body is not utf-8"),
        }
    };
    let slot = match daemon.slot(name, id) {
        Ok(slot) => slot,
        Err(e) => return Reply::text(400, format!("{e:#}")),
    };
    let mut actor = slot.lock().await;
    match actor.request(&daemon.eng, &daemon.meter, &text).await {
        Ok(value) => Reply::json(200, &value),
        Err(e) => Reply::text(500, format!("{e:#}")),
    }
}

struct HttpReq {
    method: String,
    path: String,
    body: Vec<u8>,
}

// ponytail: one request per connection, Content-Length only. Chunked bodies
// and streaming responses wait until a client needs them.
async fn read_request(sock: &mut TcpStream) -> Result<HttpReq> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    let header_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            if i > HEADER_CAP {
                bail!("headers too large");
            }
            break i;
        }
        if buf.len() > HEADER_CAP {
            bail!("headers too large");
        }
        let n = sock.read(&mut tmp).await?;
        if n == 0 {
            bail!("connection closed");
        }
        buf.extend_from_slice(&tmp[..n]);
    };
    let header = std::str::from_utf8(&buf[..header_end]).context("headers are not utf-8")?;
    let mut lines = header.lines();
    let request_line = lines.next().context("request line")?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().context("method")?.to_string();
    let target = parts.next().context("path")?;
    let path = target.split('?').next().unwrap_or("").to_string();
    let mut content_length = None;
    for line in lines {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if key.eq_ignore_ascii_case("transfer-encoding")
            && value.to_ascii_lowercase().contains("chunked")
        {
            bail!("chunked bodies are not supported");
        }
        if key.eq_ignore_ascii_case("content-length") {
            content_length = Some(
                value
                    .trim()
                    .parse::<usize>()
                    .context("bad content-length")?,
            );
        }
    }
    let len = content_length.unwrap_or(0);
    if len > BODY_CAP {
        bail!("body too large");
    }
    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < len {
        let n = sock.read(&mut tmp).await?;
        if n == 0 {
            bail!("short body");
        }
        body.extend_from_slice(&tmp[..n]);
        if body.len() > BODY_CAP {
            bail!("body too large");
        }
    }
    body.truncate(len);
    Ok(HttpReq { method, path, body })
}

async fn write_reply(sock: &mut TcpStream, reply: &Reply) -> Result<()> {
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        reply.status,
        reply.reason(),
        reply.content_type,
        reply.body.len()
    );
    sock.write_all(head.as_bytes()).await?;
    sock.write_all(reply.body.as_bytes()).await?;
    Ok(())
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
