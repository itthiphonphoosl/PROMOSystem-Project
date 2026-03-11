// controllers/tk_document.controller.js
const { getPool } = require("../config/db");

const TKHEAD_TABLE   = process.env.TKHEAD_TABLE   || "dbo.TKHead";
const TKDETAIL_TABLE = process.env.TKDETAIL_TABLE || "dbo.TKDetail";

function pad(n, len) { return String(n).padStart(len, "0"); }
function yymmdd(d) {
  const yy = String(d.getFullYear()).slice(-2);
  const mm = pad(d.getMonth() + 1, 2);
  const dd = pad(d.getDate(), 2);
  return `${yy}${mm}${dd}`;
}
function actorOf(req) {
  return { u_id: req.user?.u_id ?? null, u_name: req.user?.u_name ?? "unknown", role: req.user?.role ?? "unknown" };
}
function safeTableName(fullName) {
  const s = String(fullName || "").replace(/^[A-Za-z0-9_]+\./, "");
  if (!/^[A-Za-z0-9_]+$/.test(s)) throw new Error(`Invalid table name: ${s}`);
  return `\`${s}\``;
}

const SAFE_TKHEAD   = safeTableName(TKHEAD_TABLE);
const SAFE_TKDETAIL = safeTableName(TKDETAIL_TABLE);

// gen tk_id: TK + YYMMDD + ####
async function genTkId(conn, docDate) {
  const prefix = `TK${yymmdd(docDate)}`;

  const [rows] = await conn.query(
    `SELECT tk_id FROM ${SAFE_TKHEAD}
     WHERE tk_id LIKE ?
     ORDER BY tk_id DESC
     LIMIT 1`,
    [`${prefix}%`]
  );

  let running = 1;
  if (rows.length > 0) {
    const lastId = String(rows[0].tk_id || "");
    const tail   = lastId.slice(prefix.length);
    const n      = parseInt(tail, 10);
    if (!Number.isNaN(n)) running = n + 1;
  }

  return `${prefix}${pad(running, 4)}`;
}

exports.createTkDoc = async (req, res) => {
  const actor = actorOf(req);

  if (actor.role !== "admin") {
    return res.status(403).json({ message: "Forbidden (admin only)", actor });
  }

  try {
    const pool    = getPool();
    const part_no = String(req.body.part_no || "").trim();

    if (!actor.u_id) return res.status(401).json({ message: "Unauthorized", actor });
    if (!part_no)    return res.status(400).json({ message: "part_no is required", actor });

    const [partRows] = await pool.query(
      `SELECT part_id, part_no, part_name FROM \`part\` WHERE part_no = ? LIMIT 1`,
      [part_no]
    );
    const partRow = partRows[0];
    if (!partRow) {
      return res.status(400).json({ message: "Create failed: part_no not found", actor, error: `part_no not found: ${part_no}` });
    }

    const conn = await pool.getConnection();
    await conn.beginTransaction();

    try {
      const now   = new Date();
      const tk_id = await genTkId(conn, now);

      await conn.query(
        `INSERT INTO ${SAFE_TKHEAD} (tk_id, tk_created_at_ts, created_by_u_id, tk_status)
         VALUES (?, ?, ?, ?)`,
        [tk_id, now, Number(actor.u_id), 0]
      );

     await conn.query(
  `CALL usp_TKRunLog_Create(?, ?, ?, @run_no, @lot_no)`,
  [tk_id, Number(partRow.part_id), Number(actor.u_id)]
);
const [[outRow]] = await conn.query(`SELECT @run_no AS run_no, @lot_no AS lot_no`);

      const run_no = outRow?.run_no ?? null;
      const lot_no = outRow?.lot_no ?? null;

      if (!run_no || !lot_no) {
        throw new Error("DB did not return run_no/lot_no from usp_TKRunLog_Create");
      }

      await conn.query(
        `INSERT INTO ${SAFE_TKDETAIL}
           (tk_id, MC_id, op_sta_id, op_sc_id, u_id, part_id, lot_no, tk_status, tk_created_at_ts)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [tk_id, null, null, null, Number(actor.u_id), Number(partRow.part_id), lot_no, 0, now]
      );

      await conn.commit();
      conn.release();

      console.log(`[TKDOC_CREATE] tk_id=${tk_id} part_no=${partRow.part_no} created_by_u_id=${actor.u_id}`);

      return res.status(201).json({
        message:   "Created TK document",
        id:        tk_id,
        part_id:   Number(partRow.part_id),
        part_no:   partRow.part_no,
        part_name: partRow.part_name,
        run_no,
        lot_no,
        MC_id:        null,
        MC_name:      null,
        op_sta_id:    null,
        op_sta_name:  null,
        created_at:   now.toISOString(),
      });
    } catch (e) {
      await conn.rollback();
      conn.release();
      throw e;
    }
  } catch (err) {
    console.error("[TKDOC_CREATE][ERROR]", err);
    return res.status(500).json({ message: "Create failed", error: err.message });
  }
};

exports.createTkDocument = exports.createTkDoc;

exports.listTkDocs = async (req, res) => {
  const actor = actorOf(req);

  try {
    const pool = getPool();

    const [rows] = await pool.query(
      `SELECT
         d.tk_id, d.MC_id, d.op_sta_id, d.op_sc_id, d.u_id,
         d.part_id, p.part_no, p.part_name, d.lot_no,
         h.tk_status, h.tk_created_at_ts
       FROM ${SAFE_TKDETAIL} d
       LEFT JOIN ${SAFE_TKHEAD} h ON h.tk_id   = d.tk_id
       LEFT JOIN \`part\`       p ON p.part_id = d.part_id
       ORDER BY h.tk_created_at_ts DESC`
    );

    const items = rows.map((x) => ({
      id:     x.tk_id,
      lot_no: x.lot_no,
      detail: {
        tk_id: x.tk_id, MC_id: x.MC_id, op_sta_id: x.op_sta_id, op_sc_id: x.op_sc_id,
        u_id: x.u_id, part_id: x.part_id, part_no: x.part_no, part_name: x.part_name,
        lot_no: x.lot_no, tk_status: x.tk_status, tk_created_at_ts: x.tk_created_at_ts,
      },
    }));

    return res.json({ actor, total: items.length, items });
  } catch (err) {
    console.error("[TKDOC_LIST][ERROR]", err);
    return res.status(500).json({ message: "List failed", error: err.message });
  }
};

exports.getTkDocById = async (req, res) => {
  const actor = actorOf(req);
  const id    = String(req.params.id || "").trim();
  if (!id) return res.status(400).json({ message: "id is required", actor });

  try {
    const pool = getPool();

    const [headRows] = await pool.query(
      `SELECT tk_id, tk_status, created_by_u_id, tk_created_at_ts
       FROM ${SAFE_TKHEAD} WHERE tk_id = ? LIMIT 1`,
      [id]
    );
    const head = headRows[0];
    if (!head) return res.status(404).json({ message: "Not found", id, actor });

    const [detailRows] = await pool.query(
      `SELECT d.tk_id, d.MC_id, d.op_sta_id, d.op_sc_id, d.u_id,
              d.part_id, p.part_no, p.part_name, d.lot_no, d.tk_created_at_ts
       FROM ${SAFE_TKDETAIL} d
       LEFT JOIN \`part\` p ON p.part_id = d.part_id
       WHERE d.tk_id = ?
       ORDER BY d.tk_created_at_ts ASC
       LIMIT 1`,
      [id]
    );
    const detailRow = detailRows[0] || null;

    const [runlogRows] = await pool.query(
      `SELECT run_no, lot_no, created_at_ts, created_by_u_id, part_id
       FROM \`TKRunLog\` WHERE tk_id = ? ORDER BY created_at_ts DESC`,
      [id]
    );

    const runlogs = runlogRows.map((x) => ({
      run_no:          x.run_no ? String(x.run_no).trim() : null,
      lot_no:          x.lot_no || null,
      created_at_ts:   x.created_at_ts ? new Date(x.created_at_ts).toISOString() : null,
      created_by_u_id: x.created_by_u_id ?? null,
      part_id:         x.part_id ?? null,
    }));

    const latestRunNo = runlogs[0]?.run_no ?? null;
    const latestLotNo = runlogs[0]?.lot_no ?? (detailRow?.lot_no ?? null);

    console.log(`[TKDOC_GET] tk_id=${id} tk_status=${head.tk_status} run_no=${latestRunNo || "-"}`);

    return res.json({
      actor,
      id,
      lot_no:      latestLotNo,
      tk_status:   head.tk_status,
      is_finished: head.tk_status === 1,
      detail: detailRow ? {
        tk_id: detailRow.tk_id, MC_id: detailRow.MC_id, op_sta_id: detailRow.op_sta_id,
        op_sc_id: detailRow.op_sc_id, u_id: detailRow.u_id, part_id: detailRow.part_id,
        lot_no: detailRow.lot_no, tk_status: head.tk_status, is_finished: head.tk_status === 1,
        tk_created_at_ts: detailRow.tk_created_at_ts ? new Date(detailRow.tk_created_at_ts).toISOString() : null,
        part_no: detailRow.part_no, part_name: detailRow.part_name, run_no: latestRunNo,
      } : null,
      runlogs,
    });
  } catch (err) {
    console.error("[TKDOC_GET][ERROR]", err);
    return res.status(500).json({ message: "Get failed", actor, error: err.message });
  }
};