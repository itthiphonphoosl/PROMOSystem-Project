// controllers/op_scan_query.controller.js
// ──────────────────────────────────────────
// GET / READ endpoints สำหรับ op_scan เท่านั้น
// Start / Finish อยู่ใน op_scan.controller.js
// ──────────────────────────────────────────
const { getPool } = require("../config/db");

const OP_SCAN_TABLE  = process.env.OP_SCAN_TABLE  || process.env.OPSCAN_TABLE || "dbo.op_scan";
const TKDETAIL_TABLE = process.env.TKDETAIL_TABLE || "dbo.TKDetail";
const TRANSFER_TABLE = process.env.TRANSFER_TABLE || "dbo.t_transfer";

function safeTableName(fullName) {
  const s = String(fullName || "").replace(/^[A-Za-z0-9_]+\./, "");
  if (!/^[A-Za-z0-9_]+$/.test(s)) throw new Error(`Invalid table name: ${s}`);
  return `\`${s}\``;
}

const SAFE_OPSCAN   = safeTableName(OP_SCAN_TABLE);
const SAFE_TKDETAIL = safeTableName(TKDETAIL_TABLE);
const SAFE_TRANSFER = safeTableName(TRANSFER_TABLE);

function normalizeClientType(raw) {
  const v = String(raw || "").trim().toUpperCase();
  if (v === "HH" || v === "FLUTTER") return "HH";
  if (v === "PC" || v === "REACT")   return "PC";
  return "UNKNOWN";
}

function actorOf(req) {
  return {
    u_id:        req.user?.u_id        ?? null,
    u_firstname: req.user?.u_firstname ?? "",
    u_lastname:  req.user?.u_lastname  ?? "",
    role:        req.user?.role        ?? "unknown",
    u_type:      req.user?.u_type      ?? "unknown",
    op_sta_id:   req.user?.op_sta_id   ?? null,
    op_sta_name: req.user?.op_sta_name ?? null,
    clientType:  normalizeClientType(req.headers["x-client-type"]),
  };
}

// ─────────────────────────────────────────────────────────
// GET /api/op-scan/active
// ─────────────────────────────────────────────────────────
exports.listAllActiveOpScans = async (req, res) => {
  const actor      = actorOf(req);
  const isOperator = actor.u_type === "op";
  const opSta      = actor.op_sta_id ? String(actor.op_sta_id).trim() : "";

  if (isOperator && !opSta) {
    return res.status(400).json({ message: "Missing op_sta_id in token", actor });
  }

  try {
    const pool = getPool();

    // MySQL ไม่มี OUTER APPLY → ใช้ subquery แทน
    const whereExtra = isOperator
      ? "AND TRIM(COALESCE(s.op_sta_id, m.op_sta_id)) = ?"
      : "";
    const params = isOperator ? [opSta] : [];

    const [rows] = await pool.query(
      `SELECT
         s.op_sc_id,
         s.tk_id,
         COALESCE(s.op_sta_id, m.op_sta_id) AS op_sta_id,
         st.op_sta_name,
         s.MC_id,
         s.u_id,
         s.op_sc_total_qty,
         s.op_sc_scrap_qty,
         s.op_sc_good_qty,
         s.tf_rs_code,
         (SELECT d.lot_no FROM ${SAFE_TKDETAIL} d
          WHERE d.tk_id = s.tk_id
          ORDER BY d.tk_created_at_ts DESC LIMIT 1) AS lot_no,
         s.op_sc_ts,
         s.op_sc_finish_ts
       FROM ${SAFE_OPSCAN} s
       LEFT JOIN \`machine\`    m  ON m.MC_id      = s.MC_id
       LEFT JOIN \`op_station\` st ON st.op_sta_id = COALESCE(s.op_sta_id, m.op_sta_id)
       WHERE s.op_sc_ts       IS NOT NULL
         AND s.op_sc_finish_ts IS NULL
         ${whereExtra}
       ORDER BY s.op_sc_ts DESC
       LIMIT 200`,
      params
    );

    return res.json({ actor, active: true, count: rows.length, items: rows });
  } catch (err) {
    console.error("[OPSCAN_ACTIVE_ALL][ERROR]", err);
    return res.status(500).json({ message: "Get active list failed", actor, error: err.message });
  }
};

// ─────────────────────────────────────────────────────────
// GET /api/op-scan/:op_sc_id
// ─────────────────────────────────────────────────────────
exports.getOpScanById = async (req, res) => {
  const actor    = actorOf(req);
  const op_sc_id = String(req.params.op_sc_id || req.params.id || "").trim();
  if (!op_sc_id) return res.status(400).json({ message: "op_sc_id is required", actor });

  try {
    const pool  = getPool();
    const [rows] = await pool.query(
      `SELECT
         s.op_sc_id,
         s.tk_id,
         s.op_sta_id,
         st.op_sta_name,
         s.MC_id,
         m.MC_name,
         s.u_id,
         s.op_sc_total_qty,
         s.op_sc_scrap_qty,
         s.op_sc_good_qty,
         s.tf_rs_code,
         (SELECT d.lot_no FROM ${SAFE_TKDETAIL} d
          WHERE d.tk_id = s.tk_id
          ORDER BY d.tk_created_at_ts DESC LIMIT 1) AS lot_no,
         s.op_sc_ts,
         s.op_sc_finish_ts
       FROM ${SAFE_OPSCAN} s
       LEFT JOIN \`op_station\` st ON st.op_sta_id = s.op_sta_id
       LEFT JOIN \`machine\`    m  ON m.MC_id      = s.MC_id
       WHERE s.op_sc_id = ?
       LIMIT 1`,
      [op_sc_id]
    );

    const row = rows[0];
    if (!row) return res.status(404).json({ message: "Not found", actor, op_sc_id });

    return res.json({ actor, item: row });
  } catch (err) {
    console.error("[OPSCAN_GET][ERROR]", err);
    return res.status(500).json({ message: "Get failed", actor, error: err.message });
  }
};

// ─────────────────────────────────────────────────────────
// GET /api/op-scan/active/:tk_id
// ─────────────────────────────────────────────────────────
exports.getActiveOpScanByTkId = async (req, res) => {
  const actor  = actorOf(req);
  const tk_id  = String(req.params.tk_id || req.params.tkId || req.params.id || "").trim();
  if (!tk_id) return res.status(400).json({ message: "กรุณากรอก Tracking No.", actor });

  const isOperator = actor.u_type === "op";
  const opSta      = actor.op_sta_id ? String(actor.op_sta_id).trim() : "";
  if (isOperator && !opSta) {
    return res.status(400).json({ message: "Missing op_sta_id in token", actor });
  }

  try {
    const pool = getPool();

    // ✅ block operator ถ้าเอกสารถูกปิด
    if (isOperator) {
      const [[tkHead]] = await pool.query(
        `SELECT tk_active FROM \`TKHead\` WHERE tk_id = ? LIMIT 1`,
        [tk_id]
      );
      if (!tkHead) {
        return res.status(404).json({ message: "ไม่พบ Tracking No. นี้ในระบบ", actor, tk_id });
      }
      if (Number(tkHead.tk_active) !== 1) {
        return res.status(403).json({
          message:   "เอกสารนี้ถูกปิดใช้งานอยู่ กรุณาติดต่อ Admin",
          tk_id,
          tk_active: Number(tkHead.tk_active),
          actor,
        });
      }
    }

    const whereExtra = isOperator
      ? "AND TRIM(COALESCE(s.op_sta_id, m.op_sta_id)) = ?"
      : "";
    const params = isOperator ? [tk_id, opSta] : [tk_id];

    const [rows] = await pool.query(
      `SELECT
         s.op_sc_id,
         s.tk_id,
         COALESCE(s.op_sta_id, m.op_sta_id) AS op_sta_id,
         st.op_sta_name,
         s.MC_id,
         m.MC_name,
         s.u_id,
         s.op_sc_total_qty,
         s.op_sc_scrap_qty,
         s.op_sc_good_qty,
         s.tf_rs_code,
         (SELECT d.lot_no FROM ${SAFE_TKDETAIL} d
          WHERE d.tk_id = s.tk_id
          ORDER BY d.tk_created_at_ts DESC LIMIT 1) AS lot_no,
         s.op_sc_ts,
         s.op_sc_finish_ts
       FROM ${SAFE_OPSCAN} s
       LEFT JOIN \`machine\`    m  ON m.MC_id      = s.MC_id
       LEFT JOIN \`op_station\` st ON st.op_sta_id = COALESCE(s.op_sta_id, m.op_sta_id)
       WHERE s.tk_id            = ?
         AND s.op_sc_ts        IS NOT NULL
         AND s.op_sc_finish_ts IS NULL
         ${whereExtra}
       ORDER BY s.op_sc_ts DESC
       LIMIT 1`,
      params
    );

    const row = rows[0] || null;
    return res.json({ actor, tk_id, active: !!row, item: row });
  } catch (err) {
    console.error("[OPSCAN_ACTIVE_BY_TK][ERROR]", err);
    return res.status(500).json({ message: "Get active failed", actor, error: err.message });
  }
};

// ─────────────────────────────────────────────────────────
// GET /api/op-scan/summary/:tk_id
// ─────────────────────────────────────────────────────────
exports.getTkSummary = async (req, res) => {
  const actor = actorOf(req);
  const tk_id = String(req.params.tk_id || "").trim();
  if (!tk_id) return res.status(400).json({ message: "กรุณากรอก Tracking No.", actor });

  try {
    const pool = getPool();

    // 1) TKHead
   // ใหม่ — เพิ่ม tk_active ใน SELECT + block operator
const [headRows] = await pool.query(
  `SELECT tk_id, tk_status, tk_active, created_by_u_id, tk_created_at_ts
   FROM \`TKHead\` WHERE tk_id = ? LIMIT 1`,
  [tk_id]
);
const head = headRows[0];
if (!head) return res.status(404).json({ message: "ไม่พบ Tracking No. นี้ ไม่มีอยู่ในระบบ", actor, tk_id });
// ✅ เพิ่ม: block operator ถ้าเอกสารถูกปิด
if (Number(head.tk_active) !== 1 && actor.u_type === "op") {
  return res.status(403).json({ message: "เอกสารนี้ถูกปิดใช้งานอยู่ กรุณาติดต่อ Admin", actor, tk_id, tk_active: Number(head.tk_active) });
}

    // 2) TKDetail
    const [detailRows] = await pool.query(
      `SELECT d.tk_id, d.lot_no, d.part_id, p.part_no, p.part_name,
              d.MC_id, d.op_sta_id, d.tk_created_at_ts
       FROM ${SAFE_TKDETAIL} d
       LEFT JOIN \`part\` p ON p.part_id = d.part_id
       WHERE d.tk_id = ? ORDER BY d.tk_created_at_ts DESC LIMIT 1`,
      [tk_id]
    );
    const detail = detailRows[0] ?? null;

    // 3) op_scan history
    const [scanRows] = await pool.query(
      `SELECT
         s.op_sc_id, s.op_sta_id, st.op_sta_name,
         s.MC_id, m.MC_name,
         s.u_id, u.u_firstname, u.u_lastname,
         s.op_sc_total_qty, s.op_sc_good_qty, s.op_sc_scrap_qty,
         s.tf_rs_code, s.lot_no,
         s.op_sc_ts, s.op_sc_finish_ts,
         CASE WHEN s.op_sc_finish_ts IS NULL THEN 'IN_PROGRESS' ELSE 'DONE' END AS scan_status
       FROM ${SAFE_OPSCAN} s
       LEFT JOIN \`op_station\` st ON st.op_sta_id = s.op_sta_id
       LEFT JOIN \`machine\`    m  ON m.MC_id      = s.MC_id
       LEFT JOIN \`user\`       u  ON u.u_id       = s.u_id
       WHERE s.tk_id = ?
       ORDER BY s.op_sc_ts ASC`,
      [tk_id]
    );

    // 4) transfer history
    const [transferRows] = await pool.query(
      `SELECT
         t.transfer_id, t.from_tk_id, t.to_tk_id,
         t.from_lot_no, t.to_lot_no,
         t.tf_rs_code, tr.tf_rs_name,
         t.transfer_qty, t.op_sc_id, t.op_sta_id, s.op_sta_name,
         t.MC_id, m.MC_name,
         t.lot_parked_status,
         CASE t.lot_parked_status WHEN 0 THEN 'Active' WHEN 1 THEN 'Parked' END AS lot_status_name,
         t.color_id, cp.color_no, cp.color_name,
         t.created_by_u_id, u.u_firstname AS created_by_u_firstname, u.u_lastname AS created_by_u_lastname,
         t.transfer_ts
       FROM ${SAFE_TRANSFER} t
       LEFT JOIN \`transfer_reason\` tr ON tr.tf_rs_code = t.tf_rs_code
       LEFT JOIN \`op_station\`      s  ON s.op_sta_id   = t.op_sta_id
       LEFT JOIN \`machine\`         m  ON m.MC_id       = t.MC_id
       LEFT JOIN \`user\`            u  ON u.u_id        = t.created_by_u_id
       LEFT JOIN \`color_painting\`  cp ON cp.color_id   = t.color_id
       WHERE t.from_tk_id = ? OR t.to_tk_id = ?
       ORDER BY t.transfer_ts ASC`,
      [tk_id, tk_id]
    );

    // 5) base lot
    const [baseLotRows] = await pool.query(
      `SELECT run_no, lot_no FROM \`TKRunLog\`
       WHERE tk_id = ? ORDER BY created_at_ts ASC LIMIT 1`,
      [tk_id]
    );
    const base_lot_no = baseLotRows[0]?.lot_no ?? null;
    const base_run_no = baseLotRows[0]?.run_no ? String(baseLotRows[0].run_no).trim() : null;

    // 6) parked lots ของ TK นี้
    const [parkedRows] = await pool.query(
      `SELECT
         t.transfer_id,
         t.to_lot_no   AS parked_lot_no,
         t.from_lot_no AS came_from_lot,
         t.tf_rs_code, tr.tf_rs_name,
         t.transfer_qty AS parked_qty,
         t.op_sta_id, s.op_sta_name
       FROM ${SAFE_TRANSFER} t
       LEFT JOIN \`transfer_reason\` tr ON tr.tf_rs_code = t.tf_rs_code
       LEFT JOIN \`op_station\`      s  ON s.op_sta_id   = t.op_sta_id
       WHERE (t.from_tk_id = ? OR t.to_tk_id = ?)
         AND t.lot_parked_status = 1
       ORDER BY t.transfer_ts ASC`,
      [tk_id, tk_id]
    );

    const scans     = scanRows     || [];
    const transfers = transferRows || [];
    const parked    = parkedRows   || [];

    // 7) derived lots
    const isOperator = actor.u_type === "op";
    const actorSta   = actor.op_sta_id ? String(actor.op_sta_id).trim() : "";

    if (isOperator && !actorSta) {
      return res.status(400).json({ message: "Missing op_sta_id in token", actor });
    }

    const current_station = isOperator
      ? actorSta
      : (detail?.op_sta_id ? String(detail.op_sta_id).trim() : null);

    const lastFinished = [...scans].reverse().find(s => s.op_sc_finish_ts);
    const last_finished_op_sc_id = lastFinished?.op_sc_id ? String(lastFinished.op_sc_id).trim() : null;

    let incoming_lots = [];
    if (last_finished_op_sc_id) {
      const [inRows] = await pool.query(
        `SELECT
           t.to_lot_no AS lot_no,
           t.lot_parked_status,
           SUM(t.transfer_qty) AS qty,
           MAX(t.transfer_ts)  AS last_ts
         FROM ${SAFE_TRANSFER} t
         WHERE t.op_sc_id = ?
         GROUP BY t.to_lot_no, t.lot_parked_status
         ORDER BY MAX(t.transfer_ts) DESC`,
        [last_finished_op_sc_id]
      );
      incoming_lots = inRows || [];
    }

    let parked_lots_station_all = [];
    if (current_station) {
      const [pkStaRows] = await pool.query(
        `SELECT
           t.transfer_id, t.from_tk_id, t.to_tk_id,
           t.to_lot_no   AS parked_lot_no,
           t.from_lot_no AS came_from_lot,
           t.tf_rs_code, tr.tf_rs_name AS parked_reason,
           t.transfer_qty AS parked_qty,
           t.op_sta_id, s.op_sta_name,
           t.transfer_ts  AS parked_at
         FROM ${SAFE_TRANSFER} t
         LEFT JOIN \`transfer_reason\` tr ON tr.tf_rs_code = t.tf_rs_code
         LEFT JOIN \`op_station\`      s  ON s.op_sta_id   = t.op_sta_id
         WHERE t.op_sta_id = ?
           AND t.lot_parked_status = 1
         ORDER BY t.transfer_ts DESC`,
        [current_station]
      );
      parked_lots_station_all = pkStaRows || [];
    }

    const totalGood    = scans.reduce((a, s) => a + (s.op_sc_good_qty  || 0), 0);
    const totalScrap   = scans.reduce((a, s) => a + (s.op_sc_scrap_qty || 0), 0);
    const stationsDone = [...new Set(
      scans.filter(s => s.op_sc_finish_ts).map(s => s.op_sta_id).filter(Boolean)
    )];

    const tk_status_label = {
      0: "NOT_STARTED", 1: "FINISHED", 2: "PARTIAL_DONE", 3: "IN_PROGRESS", 4: "CANCELLED",
    }[head.tk_status] ?? "UNKNOWN";

   return res.json({
  actor,
  tk_id:            head.tk_id,
  tk_status:        head.tk_status,
  tk_status_label,
  is_finished:      head.tk_status === 1,
  tk_active:        Number(head.tk_active),
  is_active:        Number(head.tk_active) === 1,
  tk_created_at_ts: head.tk_created_at_ts ? new Date(head.tk_created_at_ts).toISOString() : null,

      base:    { run_no: base_run_no, lot_no: base_lot_no },
      current: detail ? {
        lot_no: detail.lot_no, part_id: detail.part_id,
        part_no: detail.part_no, part_name: detail.part_name,
        MC_id: detail.MC_id, op_sta_id: detail.op_sta_id,
      } : null,

      current_station,
      last_finished_op_sc_id,
      incoming_lots,
      parked_lots_station_all,

      summary: {
        total_scans:       scans.length,
        total_good:        totalGood,
        total_scrap:       totalScrap,
        stations_done:     stationsDone,
        parked_lots_count: parked.length,
      },

      scans,
      transfers,
      parked_lots: parked,
    });
  } catch (err) {
    console.error("[TK_SUMMARY][ERROR]", err);
    return res.status(500).json({ message: "Get summary failed", actor, error: err.message });
  }
};

// ─────────────────────────────────────────────────────────
// GET /api/op-scan/parked?op_sta_id=STA001
// ─────────────────────────────────────────────────────────
exports.getParkedLots = async (req, res) => {
  const actor      = actorOf(req);
  const isOperator = actor.u_type === "op";

  const op_sta_id = isOperator
    ? (actor.op_sta_id ? String(actor.op_sta_id).trim() : "")
    : String(req.query.op_sta_id || "").trim();

  if (!op_sta_id) {
    return res.status(400).json({ message: "op_sta_id is required", actor });
  }

  try {
    const pool   = getPool();
    const [rows] = await pool.query(
      `SELECT
         t.transfer_id, t.from_tk_id, t.to_tk_id,
         t.to_lot_no   AS parked_lot_no,
         t.from_lot_no AS came_from_lot,
         t.tf_rs_code, tr.tf_rs_name AS parked_reason,
         t.transfer_qty AS parked_qty,
         t.op_sta_id, s.op_sta_name,
         t.transfer_ts  AS parked_at
       FROM ${SAFE_TRANSFER} t
       LEFT JOIN \`transfer_reason\` tr ON tr.tf_rs_code = t.tf_rs_code
       LEFT JOIN \`op_station\`      s  ON s.op_sta_id   = t.op_sta_id
       WHERE t.op_sta_id         = ?
         AND t.lot_parked_status = 1
       ORDER BY t.transfer_ts DESC`,
      [op_sta_id]
    );

    return res.json({ actor, op_sta_id, count: rows.length, parked_lots: rows });
  } catch (err) {
    console.error("[PARKED_LOTS][ERROR]", err);
    return res.status(500).json({ message: "Get parked lots failed", actor, error: err.message });
  }
};