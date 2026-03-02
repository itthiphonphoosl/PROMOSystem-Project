// controllers/op_scan_query.controller.js
// ──────────────────────────────────────────
// GET / READ endpoints สำหรับ op_scan เท่านั้น
// Start / Finish อยู่ใน op_scan.controller.js
// ──────────────────────────────────────────
const sql = require("mssql");
const { getPool } = require("../config/db");

const OP_SCAN_TABLE  = process.env.OP_SCAN_TABLE || process.env.OPSCAN_TABLE || "dbo.op_scan";
const TKDETAIL_TABLE = process.env.TKDETAIL_TABLE || "dbo.TKDetail";
const TRANSFER_TABLE = process.env.TRANSFER_TABLE || "dbo.t_transfer";

function safeTableName(fullName) {
  const s = String(fullName || "");
  if (!/^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$/.test(s)) throw new Error(`Invalid table name: ${s}`);
  return s;
}

const SAFE_OPSCAN    = safeTableName(OP_SCAN_TABLE);
const SAFE_TKDETAIL  = safeTableName(TKDETAIL_TABLE);
const SAFE_TRANSFER  = safeTableName(TRANSFER_TABLE);

function normalizeClientType(raw) {
  const v = String(raw || "").trim().toUpperCase();
  if (v === "HH" || v === "FLUTTER") return "HH";
  if (v === "PC" || v === "REACT")   return "PC";
  return "UNKNOWN";
}

function actorOf(req) {
  return {
    u_id:        req.user?.u_id        ?? null,
    u_name:      req.user?.u_name      ?? "unknown",
    role:        req.user?.role        ?? "unknown",
    u_type:      req.user?.u_type      ?? "unknown",
    op_sta_id:   req.user?.op_sta_id   ?? null,
    op_sta_name: req.user?.op_sta_name ?? null,
    clientType:  normalizeClientType(req.headers["x-client-type"]),
  };
}

// ─────────────────────────────────────────────────────────
// GET /api/op-scans/active
// ดู op_scan ที่ยัง active (finish_ts IS NULL) ทั้งหมด
// operator เห็นเฉพาะ station ตัวเอง, admin เห็นทั้งหมด
// ─────────────────────────────────────────────────────────
exports.listAllActiveOpScans = async (req, res) => {
  const actor      = actorOf(req);
  const isOperator = actor.u_type === "op";
  const opSta      = actor.op_sta_id ? String(actor.op_sta_id).trim() : "";

  if (isOperator && !opSta) {
    return res.status(400).json({ message: "Missing op_sta_id in token", actor });
  }

  try {
    const pool = await getPool();
    const reqQ = pool.request();
    if (isOperator) reqQ.input("op_sta_id", sql.VarChar(20), opSta);

    const r = await reqQ.query(`
      SELECT TOP (200)
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
        td.lot_no,
        s.op_sc_ts,
        s.op_sc_finish_ts
      FROM ${SAFE_OPSCAN} s WITH (NOLOCK)
      LEFT JOIN dbo.machine    m  ON m.MC_id      = s.MC_id
      LEFT JOIN dbo.op_station st ON st.op_sta_id = COALESCE(s.op_sta_id, m.op_sta_id)
      OUTER APPLY (
        SELECT TOP 1 d.lot_no
        FROM ${SAFE_TKDETAIL} d WITH (NOLOCK)
        WHERE d.tk_id = s.tk_id
        ORDER BY d.tk_created_at_ts DESC
      ) td
      WHERE s.op_sc_ts       IS NOT NULL
        AND s.op_sc_finish_ts IS NULL
        ${isOperator
          ? "AND COALESCE(LTRIM(RTRIM(s.op_sta_id)), LTRIM(RTRIM(m.op_sta_id))) = @op_sta_id"
          : ""}
      ORDER BY s.op_sc_ts DESC
    `);

    return res.json({ actor, active: true, count: r.recordset.length, items: r.recordset });
  } catch (err) {
    console.error("[OPSCAN_ACTIVE_ALL][ERROR]", err);
    return res.status(500).json({ message: "Get active list failed", actor, error: err.message });
  }
};

// ─────────────────────────────────────────────────────────
// GET /api/op-scans/:op_sc_id
// ดู op_scan รายการเดียวตาม op_sc_id
// ─────────────────────────────────────────────────────────
exports.getOpScanById = async (req, res) => {
  const actor    = actorOf(req);
  const op_sc_id = String(req.params.op_sc_id || req.params.id || "").trim();
  if (!op_sc_id) return res.status(400).json({ message: "op_sc_id is required", actor });

  try {
    const pool = await getPool();
    const r    = await pool
      .request()
      .input("op_sc_id", sql.Char(12), op_sc_id)
      .query(`
        SELECT TOP 1
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
          lot_latest.lot_no,
          s.op_sc_ts,
          s.op_sc_finish_ts
        FROM ${SAFE_OPSCAN} s WITH (NOLOCK)
        LEFT JOIN dbo.op_station st ON st.op_sta_id = s.op_sta_id
        LEFT JOIN dbo.machine    m  ON m.MC_id      = s.MC_id
        OUTER APPLY (
          SELECT TOP 1 d.lot_no
          FROM ${SAFE_TKDETAIL} d WITH (NOLOCK)
          WHERE d.tk_id = s.tk_id
          ORDER BY d.tk_created_at_ts DESC
        ) lot_latest
        WHERE s.op_sc_id = @op_sc_id
      `);

    const row = r.recordset?.[0];
    if (!row) return res.status(404).json({ message: "Not found", actor, op_sc_id });

    return res.json({ actor, item: row });
  } catch (err) {
    console.error("[OPSCAN_GET][ERROR]", err);
    return res.status(500).json({ message: "Get failed", actor, error: err.message });
  }
};

// ─────────────────────────────────────────────────────────
// GET /api/op-scans/active/:tk_id
// ดู op_scan ที่ active อยู่ของ TK หนึ่งใบ
// ─────────────────────────────────────────────────────────
exports.getActiveOpScanByTkId = async (req, res) => {
  const actor  = actorOf(req);
  const tk_id  = String(req.params.tk_id || req.params.tkId || req.params.id || "").trim();
  if (!tk_id) return res.status(400).json({ message: "tk_id is required", actor });

  const isOperator = actor.u_type === "op";
  const opSta      = actor.op_sta_id ? String(actor.op_sta_id).trim() : "";
  if (isOperator && !opSta) {
    return res.status(400).json({ message: "Missing op_sta_id in token", actor });
  }

  try {
    const pool = await getPool();
    const rq   = pool.request().input("tk_id", sql.VarChar(20), tk_id);
    if (isOperator) rq.input("op_sta_id", sql.VarChar(20), opSta);

    const r = await rq.query(`
      SELECT TOP 1
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
        lot_latest.lot_no,
        s.op_sc_ts,
        s.op_sc_finish_ts
      FROM ${SAFE_OPSCAN} s WITH (NOLOCK)
      LEFT JOIN dbo.machine    m  ON m.MC_id      = s.MC_id
      LEFT JOIN dbo.op_station st ON st.op_sta_id = COALESCE(s.op_sta_id, m.op_sta_id)
      OUTER APPLY (
        SELECT TOP 1 d.lot_no
        FROM ${SAFE_TKDETAIL} d WITH (NOLOCK)
        WHERE d.tk_id = s.tk_id
        ORDER BY d.tk_created_at_ts DESC
      ) lot_latest
      WHERE s.tk_id             = @tk_id
        AND s.op_sc_ts         IS NOT NULL
        AND s.op_sc_finish_ts  IS NULL
        ${isOperator
          ? "AND COALESCE(LTRIM(RTRIM(s.op_sta_id)), LTRIM(RTRIM(m.op_sta_id))) = @op_sta_id"
          : ""}
      ORDER BY s.op_sc_ts DESC
    `);

    const row = r.recordset?.[0] || null;
    return res.json({ actor, tk_id, active: !!row, item: row });
  } catch (err) {
    console.error("[OPSCAN_ACTIVE_BY_TK][ERROR]", err);
    return res.status(500).json({ message: "Get active failed", actor, error: err.message });
  }
};

// ─────────────────────────────────────────────────────────
// GET /api/tk-documents/:tk_id/summary
// ภาพรวม TK ทั้งใบ — scans, transfers, lot status
// ─────────────────────────────────────────────────────────
exports.getTkSummary = async (req, res) => {
  const actor = actorOf(req);
  const tk_id = String(req.params.tk_id || "").trim();
  if (!tk_id) return res.status(400).json({ message: "tk_id is required", actor });

  try {
    const pool = await getPool();

    // 1) TKHead
    const headR = await pool.request()
      .input("tk_id", sql.VarChar(20), tk_id)
      .query(`
        SELECT TOP 1 tk_id, tk_status, created_by_u_id, tk_created_at_ts
        FROM dbo.TKHead WITH (NOLOCK)
        WHERE tk_id = @tk_id
      `);

    const head = headR.recordset?.[0];
    if (!head) return res.status(404).json({ message: "tk_id not found", actor, tk_id });

    // 2) TKDetail
    const detailR = await pool.request()
      .input("tk_id", sql.VarChar(20), tk_id)
      .query(`
        SELECT TOP 1
          d.tk_id, d.lot_no, d.part_id,
          p.part_no, p.part_name,
          d.MC_id, d.op_sta_id, d.tk_created_at_ts
        FROM ${SAFE_TKDETAIL} d WITH (NOLOCK)
        LEFT JOIN dbo.part p ON p.part_id = d.part_id
        WHERE d.tk_id = @tk_id
        ORDER BY d.tk_created_at_ts DESC
      `);
    const detail = detailR.recordset?.[0] ?? null;

    // 3) op_scan history
    const scanR = await pool.request()
      .input("tk_id", sql.VarChar(20), tk_id)
      .query(`
        SELECT
          s.op_sc_id, s.op_sta_id, st.op_sta_name,
          s.MC_id, m.MC_name,
          s.u_id, u.u_name,
          s.op_sc_total_qty, s.op_sc_good_qty, s.op_sc_scrap_qty,
          s.tf_rs_code, s.lot_no,
          s.op_sc_ts, s.op_sc_finish_ts,
          CASE WHEN s.op_sc_finish_ts IS NULL THEN 'IN_PROGRESS' ELSE 'DONE' END AS scan_status
        FROM ${SAFE_OPSCAN} s WITH (NOLOCK)
        LEFT JOIN dbo.op_station st ON st.op_sta_id = s.op_sta_id
        LEFT JOIN dbo.machine    m  ON m.MC_id      = s.MC_id
        LEFT JOIN dbo.[user]     u  ON u.u_id       = s.u_id
        WHERE s.tk_id = @tk_id
        ORDER BY s.op_sc_ts ASC
      `);

    // 4) transfer history — รวม lot_parked_status และ op_sta_id (field ใหม่)
    const transferR = await pool.request()
      .input("tk_id", sql.VarChar(20), tk_id)
      .query(`
        SELECT
          t.transfer_id,
          t.from_lot_no,
          t.to_lot_no,
          t.tf_rs_code,
          tr.tf_rs_name,
          t.transfer_qty,
          t.op_sc_id,
          t.op_sta_id,
          s.op_sta_name,
          t.MC_id,
          m.MC_name,
          t.lot_parked_status,
          CASE t.lot_parked_status
            WHEN 0 THEN 'Active'
            WHEN 1 THEN 'Parked'
          END AS lot_status_name,
          t.created_by_u_id,
          u.u_name AS created_by_u_name,
          t.transfer_ts
        FROM ${SAFE_TRANSFER} t WITH (NOLOCK)
        LEFT JOIN dbo.transfer_reason tr ON tr.tf_rs_code = t.tf_rs_code
        LEFT JOIN dbo.op_station      s  ON s.op_sta_id   = t.op_sta_id
        LEFT JOIN dbo.machine         m  ON m.MC_id       = t.MC_id
        LEFT JOIN dbo.[user]          u  ON u.u_id        = t.created_by_u_id
        WHERE t.from_tk_id = @tk_id OR t.to_tk_id = @tk_id
        ORDER BY t.transfer_ts ASC
      `);

    // 5) base lot (lot แรกสุด)
    const baseLotR = await pool.request()
      .input("tk_id", sql.VarChar(20), tk_id)
      .query(`
        SELECT TOP 1 run_no, lot_no
        FROM dbo.TKRunLog WITH (NOLOCK)
        WHERE tk_id = @tk_id
        ORDER BY created_at_ts ASC
      `);
    const base_lot_no = baseLotR.recordset?.[0]?.lot_no ?? null;
    const base_run_no = baseLotR.recordset?.[0]?.run_no
      ? String(baseLotR.recordset[0].run_no).trim()
      : null;

    // 6) parked lots ของ TK นี้
    const parkedR = await pool.request()
      .input("tk_id", sql.VarChar(20), tk_id)
      .query(`
        SELECT
          t.transfer_id,
          t.to_lot_no       AS parked_lot_no,
          t.from_lot_no     AS came_from_lot,
          t.tf_rs_code,
          tr.tf_rs_name,
          t.transfer_qty    AS parked_qty,
          t.op_sta_id,
          s.op_sta_name
        FROM ${SAFE_TRANSFER} t WITH (NOLOCK)
        LEFT JOIN dbo.transfer_reason tr ON tr.tf_rs_code = t.tf_rs_code
        LEFT JOIN dbo.op_station      s  ON s.op_sta_id   = t.op_sta_id
        WHERE (t.from_tk_id = @tk_id OR t.to_tk_id = @tk_id)
          AND t.lot_parked_status = 1
        ORDER BY t.transfer_ts ASC
      `);

    const scans     = scanR.recordset     || [];
    const transfers = transferR.recordset || [];
    const parked    = parkedR.recordset   || [];


// 7) derived lots for HH/Flutter (no need for clients to "guess" from transfers)

// ✅ operator ต้องยึด station จาก token (actor.op_sta_id)
// เพราะ TKDetail อาจยังเป็น station เก่า ทำให้ไปดึง parked ของคนละ station
const isOperator = actor.u_type === "op";
const actorSta   = actor.op_sta_id ? String(actor.op_sta_id).trim() : "";

if (isOperator && !actorSta) {
  return res.status(400).json({ message: "Missing op_sta_id in token", actor });
}

// ✅ operator: ใช้ station ของคนที่ login
// ✅ admin: fallback ใช้ station ล่าสุดของเอกสาร (เหมือนเดิม)
const current_station = isOperator
  ? actorSta
  : (detail?.op_sta_id ? String(detail.op_sta_id).trim() : null);
    // last finished scan (used to find lots that were produced by the previous station)
    const lastFinished = [...scans].reverse().find(s => s.op_sc_finish_ts);
    const last_finished_op_sc_id = lastFinished?.op_sc_id ? String(lastFinished.op_sc_id).trim() : null;

    // incoming lots = lots produced by last finished op_sc_id (active + parked)
    let incoming_lots = [];
    if (last_finished_op_sc_id) {
      const inR = await pool.request()
        .input("op_sc_id", sql.Char(12), last_finished_op_sc_id)
        .query(`
          SELECT
            t.to_lot_no AS lot_no,
            t.lot_parked_status,
            SUM(t.transfer_qty) AS qty,
            MAX(t.transfer_ts) AS last_ts
          FROM ${SAFE_TRANSFER} t WITH (NOLOCK)
          WHERE t.op_sc_id = @op_sc_id
          GROUP BY t.to_lot_no, t.lot_parked_status
          ORDER BY MAX(t.transfer_ts) DESC
        `);
      incoming_lots = inR.recordset || [];
    }

    // parked lots in the current station (ALL documents) — for "pull out" use cases
    let parked_lots_station_all = [];
    if (current_station) {
      const pkStaR = await pool.request()
        .input("op_sta_id", sql.VarChar(20), current_station)
        .query(`
          SELECT
            t.transfer_id,
            t.from_tk_id,
            t.to_tk_id,
            t.to_lot_no       AS parked_lot_no,
            t.from_lot_no     AS came_from_lot,
            t.tf_rs_code,
            tr.tf_rs_name     AS parked_reason,
            t.transfer_qty    AS parked_qty,
            t.op_sta_id,
            s.op_sta_name,
            t.transfer_ts     AS parked_at
          FROM ${SAFE_TRANSFER} t WITH (NOLOCK)
          LEFT JOIN dbo.transfer_reason tr ON tr.tf_rs_code = t.tf_rs_code
          LEFT JOIN dbo.op_station      s  ON s.op_sta_id   = t.op_sta_id
          WHERE t.op_sta_id = @op_sta_id
            AND t.lot_parked_status = 1
          ORDER BY t.transfer_ts DESC
        `);
      parked_lots_station_all = pkStaR.recordset || [];
    }

    const totalGood  = scans.reduce((a, s) => a + (s.op_sc_good_qty  || 0), 0);
    const totalScrap = scans.reduce((a, s) => a + (s.op_sc_scrap_qty || 0), 0);
    const stationsDone = [...new Set(
      scans.filter(s => s.op_sc_finish_ts).map(s => s.op_sta_id).filter(Boolean)
    )];

    const tk_status_label = {
      0: "NOT_STARTED", 1: "FINISHED", 2: "PARTIAL_DONE", 3: "IN_PROGRESS",
    }[head.tk_status] ?? "UNKNOWN";

    return res.json({
      actor,
      tk_id:            head.tk_id,
      tk_status:        head.tk_status,
      tk_status_label,
      is_finished:      head.tk_status === 1,
      tk_created_at_ts: head.tk_created_at_ts
        ? new Date(head.tk_created_at_ts).toISOString() : null,

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
        total_scans:        scans.length,
        total_good:         totalGood,
        total_scrap:        totalScrap,
        stations_done:      stationsDone,
        parked_lots_count:  parked.length,
      },

      scans,
      transfers,
      parked_lots: parked,   // ← ใหม่: แสดง lot ที่พักอยู่ทั้งหมดของ TK นี้
    });
  } catch (err) {
    console.error("[TK_SUMMARY][ERROR]", err);
    return res.status(500).json({ message: "Get summary failed", actor, error: err.message });
  }
};

// ─────────────────────────────────────────────────────────
// GET /api/op-scans/parked?op_sta_id=STA001
// ดู Lot ที่พักอยู่ใน Station ของ Operator (ใช้ตอนจะดึงออก)
// ─────────────────────────────────────────────────────────
exports.getParkedLots = async (req, res) => {
  const actor      = actorOf(req);
  const isOperator = actor.u_type === "op";

  // operator ใช้จาก token, admin ระบุ query string ได้
  const op_sta_id = isOperator
    ? (actor.op_sta_id ? String(actor.op_sta_id).trim() : "")
    : String(req.query.op_sta_id || "").trim();

  if (!op_sta_id) {
    return res.status(400).json({ message: "op_sta_id is required", actor });
  }

  try {
    const pool = await getPool();
    const r    = await pool.request()
      .input("op_sta_id", sql.VarChar(20), op_sta_id)
      .query(`
        SELECT
          t.transfer_id,
          t.from_tk_id,
          t.to_tk_id,
          t.to_lot_no       AS parked_lot_no,
          t.from_lot_no     AS came_from_lot,
          t.tf_rs_code,
          tr.tf_rs_name     AS parked_reason,
          t.transfer_qty    AS parked_qty,
          t.op_sta_id,
          s.op_sta_name,
          t.transfer_ts     AS parked_at
        FROM ${SAFE_TRANSFER} t WITH (NOLOCK)
        LEFT JOIN dbo.transfer_reason tr ON tr.tf_rs_code = t.tf_rs_code
        LEFT JOIN dbo.op_station      s  ON s.op_sta_id   = t.op_sta_id
        WHERE t.op_sta_id         = @op_sta_id
          AND t.lot_parked_status = 1
        ORDER BY t.transfer_ts DESC
      `);

    return res.json({
      actor,
      op_sta_id,
      count:       r.recordset.length,
      parked_lots: r.recordset,
    });
  } catch (err) {
    console.error("[PARKED_LOTS][ERROR]", err);
    return res.status(500).json({ message: "Get parked lots failed", actor, error: err.message });
  }
};