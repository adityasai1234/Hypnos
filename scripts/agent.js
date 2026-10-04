// env.AI.fetch is a commit point. Every write made before the call is durable,
// even if the handler traps later. A trap rolls back only the writes since the
// last model call. Same rule as Durable Objects: writes with no await on
// outside I/O between them commit atomically.
export default {
  async fetch(req, env) {
    if (req.probe) {
      const out = [];
      for (const url of req.probe) {
        try {
          env.AI.fetch(url, { method: "POST", body: "{}" });
          out.push("allowed");
        } catch (e) {
          out.push(String(e && e.message ? e.message : e));
        }
      }
      return { out };
    }
    env.storage.sql("CREATE TABLE IF NOT EXISTS messages(role TEXT, body TEXT)");
    if (req.trap_after) {
      env.storage.sql("INSERT INTO messages(role, body) VALUES (?, ?)", "user", "kept");
      env.AI.fetch(req.url, { method: "POST", body: "{}" });
      env.storage.sql("INSERT INTO messages(role, body) VALUES (?, ?)", "user", "lost");
      throw new Error("trap");
    }
    env.storage.sql(
      "INSERT INTO messages(role, body) VALUES (?, ?)",
      "user",
      req.text || ""
    );
    const resp = env.AI.fetch(req.url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ text: req.text || "" }),
    });
    env.storage.sql(
      "INSERT INTO messages(role, body) VALUES (?, ?)",
      "assistant",
      resp.body
    );
    env.storage.setAlarm(Date.now() + 60000);
    return { ok: true, body: resp.body };
  },
};
