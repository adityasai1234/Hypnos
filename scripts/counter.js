let heap = 0;
export default {
  async fetch(req, env) {
    heap++;
    if (req.trap === "heap" || req.trap === "loop" || req.trap === "recurse" || req.trap === "throw") {
      env.storage.sql("CREATE TABLE IF NOT EXISTS c(n INTEGER)");
      env.storage.sql("INSERT INTO c VALUES (1)");
      if (req.trap === "heap") {
        const a = [];
        for (;;) a.push(new Array(1e6));
      }
      if (req.trap === "loop") {
        for (;;) {}
      }
      if (req.trap === "recurse") {
        function f() { f(); }
        f();
      }
      throw new Error("nope");
    }
    if (req.op === "read") {
      const rows = env.storage.sql("SELECT count(*) AS n FROM c");
      const n = rows.length ? rows[0].n : 0;
      return { heap, sql: n };
    }
    env.storage.sql("CREATE TABLE IF NOT EXISTS c(n INTEGER)");
    env.storage.sql("INSERT INTO c VALUES (1)");
    const rows = env.storage.sql("SELECT count(*) AS n FROM c");
    if (req.burn_ms) {
      const t = Date.now();
      while (Date.now() - t < req.burn_ms) {}
    }
    if (req.alarm) env.storage.setAlarm(Date.now() + 60000);
    return { heap, sql: rows[0].n };
  },
};
