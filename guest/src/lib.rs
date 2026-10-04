//! QuickJS guest. Script source is data. One fresh instance per wake, so the
//! heap starts empty. Host imports are synchronous from JavaScript's point of
//! view; Wasmtime suspends the fiber under `hypnos.fetch`.

use std::cell::RefCell;

use rquickjs::{Context, Ctx, Function, Module, Object, Runtime, Value};

thread_local! {
    static VM: RefCell<Option<Vm>> = const { RefCell::new(None) };
    static LAST_ERROR: RefCell<String> = const { RefCell::new(String::new()) };
}

struct Vm {
    // Owns a runtime ref so dropping the instance (and this thread-local) is
    // what frees the heap. Context holds its own ref; this one is the explicit owner.
    #[allow(dead_code)]
    runtime: Runtime,
    context: Context,
}

#[link(wasm_import_module = "hypnos")]
unsafe extern "C" {
    fn sql(q_ptr: u32, q_len: u32, p_ptr: u32, p_len: u32) -> u64;
    fn set_alarm(at_ms: f64);
    fn delete_alarm();
    fn fetch(ptr: u32, len: u32) -> u64;
}

const SHIM: &str = r#"
globalThis.env = {
  storage: {
    sql(q, ...p) {
      const r = JSON.parse(globalThis.__sql(q, JSON.stringify(p)));
      if (r.err) throw new Error(r.err);
      return r.rows;
    },
    setAlarm(ms) { globalThis.__setAlarm(ms); },
    deleteAlarm() { globalThis.__delAlarm(); }
  },
  AI: {
    fetch(url, opts) {
      const o = opts || {};
      const r = JSON.parse(globalThis.__fetch(JSON.stringify({
        url: url,
        method: o.method || "GET",
        headers: o.headers || {},
        body: o.body == null ? null : o.body
      })));
      if (r.err) throw new Error(r.err);
      return r;
    }
  }
};
globalThis.__invoke = async (reqStr) => {
  try {
    const req = JSON.parse(reqStr);
    const result = await globalThis.__handler(req, globalThis.env);
    return JSON.stringify({ ok: result === undefined ? null : result });
  } catch (e) {
    const msg = (e && e.message) ? e.message : String(e);
    return JSON.stringify({ trap: msg });
  }
};
"#;

#[no_mangle]
pub extern "C" fn alloc(len: u32) -> u32 {
    let mut buf = vec![0u8; len as usize];
    let ptr = buf.as_mut_ptr() as u32;
    std::mem::forget(buf);
    ptr
}

#[no_mangle]
pub extern "C" fn init(ptr: u32, len: u32) -> u32 {
    let src = read_bytes(ptr, len);
    match init_inner(&src) {
        Ok(()) => 0,
        Err(e) => {
            set_error(e);
            1
        }
    }
}

#[no_mangle]
pub extern "C" fn call(ptr: u32, len: u32) -> u64 {
    let req = read_bytes(ptr, len);
    match call_inner(&req) {
        Ok(out) => pack_string(&out),
        Err(e) => {
            set_error(e);
            0
        }
    }
}

#[no_mangle]
pub extern "C" fn last_error() -> u64 {
    LAST_ERROR.with(|e| pack_string(&e.borrow()))
}

fn init_inner(src: &str) -> Result<(), String> {
    let runtime = Runtime::new().map_err(|e| e.to_string())?;
    // Wasmtime StoreLimits is the memory cap. QuickJS's own limit would hide it.
    runtime.set_memory_limit(0);
    runtime.set_max_stack_size(256 * 1024);
    let context = Context::full(&runtime).map_err(|e| e.to_string())?;
    context.with(|ctx| install(ctx.clone(), src).map_err(|e| explain(&ctx, e)))?;
    VM.with(|slot| *slot.borrow_mut() = Some(Vm { runtime, context }));
    Ok(())
}

fn install(ctx: Ctx<'_>, src: &str) -> rquickjs::Result<()> {
    let globals = ctx.globals();
    globals.set(
        "__sql",
        Function::new(ctx.clone(), |q: String, p: String| -> rquickjs::Result<String> {
            let packed = unsafe {
                sql(
                    q.as_ptr() as u32,
                    q.len() as u32,
                    p.as_ptr() as u32,
                    p.len() as u32,
                )
            };
            Ok(unpack_string(packed))
        })?,
    )?;
    globals.set(
        "__setAlarm",
        Function::new(ctx.clone(), |ms: f64| {
            unsafe { set_alarm(ms) };
        })?,
    )?;
    globals.set(
        "__delAlarm",
        Function::new(ctx.clone(), || unsafe { delete_alarm() })?,
    )?;
    globals.set(
        "__fetch",
        Function::new(ctx.clone(), |req: String| -> rquickjs::Result<String> {
            let packed = unsafe { fetch(req.as_ptr() as u32, req.len() as u32) };
            Ok(unpack_string(packed))
        })?,
    )?;
    ctx.eval::<(), _>(SHIM)?;

    let module = Module::declare(ctx.clone(), "user", src)?;
    let (module, promise) = module.eval()?;
    promise.finish::<()>()?;
    let default: Object = module.get("default")?;
    let fetch_fn: Function = default.get("fetch")?;
    globals.set("__handler", fetch_fn)?;
    Ok(())
}

fn call_inner(req: &str) -> Result<String, String> {
    VM.with(|slot| {
        let vm = slot.borrow();
        let vm = vm.as_ref().ok_or("init was not called")?;
        vm.context.with(|ctx| {
            let func: Function = ctx
                .globals()
                .get("__invoke")
                .map_err(|e| explain(&ctx, e))?;
            let value: Value = func.call((req,)).map_err(|e| explain(&ctx, e))?;
            let Some(promise) = value.as_promise() else {
                return Err("invoke did not return a promise".into());
            };
            // finish drives the job queue. The loop is wasm, so the epoch
            // deadline still applies if a microtask chain never settles.
            promise.finish().map_err(|e| explain(&ctx, e))
        })
    })
}

fn explain(ctx: &Ctx<'_>, err: rquickjs::Error) -> String {
    if matches!(err, rquickjs::Error::Exception) {
        let caught = ctx.catch();
        if let Some(exc) = caught.as_exception() {
            if let Some(msg) = exc.message() {
                return msg;
            }
            return exc.to_string();
        }
    }
    err.to_string()
}

fn set_error(e: String) {
    LAST_ERROR.with(|s| *s.borrow_mut() = e);
}

fn read_bytes(ptr: u32, len: u32) -> String {
    if len == 0 {
        return String::new();
    }
    let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    String::from_utf8_lossy(bytes).into_owned()
}

fn unpack_string(packed: u64) -> String {
    if packed == 0 {
        return String::new();
    }
    let ptr = (packed >> 32) as u32;
    let len = packed as u32;
    read_bytes(ptr, len)
}

fn pack_string(s: &str) -> u64 {
    let len = s.len() as u32;
    let ptr = alloc(len);
    unsafe {
        std::ptr::copy_nonoverlapping(s.as_ptr(), ptr as *mut u8, s.len());
    }
    ((ptr as u64) << 32) | len as u64
}
