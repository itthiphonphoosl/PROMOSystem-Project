// controllers/op_scan.controller.js
const sql = require("mssql");
const { getPool } = require("../config/db");

const OP_SCAN_TABLE =
  process.env.OP_SCAN_TABLE || process.env.OPSCAN_TABLE || "dbo.op_scan";
const TKDETAIL_TABLE = process.env.TKDETAIL_TABLE || "dbo.TKDetail";
const MACHINE_TABLE = process.env.MACHINE_TABLE || "dbo.machine";
const PART_TABLE = process.env.PART_TABLE || "dbo.part";
const TRANSFER_TABLE = process.env.TRANSFER_TABLE || "dbo.t_transfer";

const SAFE_OPSCAN = safeTableName(OP_SCAN_TABLE);
const SAFE_TKDETAIL = safeTableName(TKDETAIL_TABLE);
const SAFE_MACHINE = safeTableName(MACHINE_TABLE);
const SAFE_PART = safeTableName(PART_TABLE);
const SAFE_TRANSFER = safeTableName(TRANSFER_TABLE);

function safeTableName(fullName) {
  const s = String(fullName || "");
  if (!/^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$/.test(s)) {
    throw new Error(`Invalid table name: ${s}`);
  }
  return s;
}
function pad(n, len) {
  return String(n).padStart(len, "0");
}
function yymmdd(d) {
  const yy = String(d.getFullYear()).slice(-2);
  const mm = pad(d.getMonth() + 1, 2);
  const dd = pad(d.getDate(), 2);
  return `${yy}${mm}${dd}`;
}

function normalizeClientType(raw) {
  const v = String(raw || "").trim().toUpperCase();
  if (v === "HH" || v === "FLUTTER") return "HH";
  if (v === "PC" || v === "REACT") return "PC";
  return "UNKNOWN";
}

function actorOf(req) {
  return {
    u_id: req.user?.u_id ?? null,
    u_name: req.user?.u_name ?? "unknown",
    role: req.user?.role ?? "unknown",
    u_type: req.user?.u_type ?? "unknown",
    op_sta_id: req.user?.op_sta_id ?? null,
    op_sta_name: req.user?.op_sta_name ?? null,
    clientType: normalizeClientType(req.headers["x-client-type"]),
  };
}

function forbid(res, message, actor) {
  return res.status(403).json({ message, actor });
}

async function genOpScId(tx, now) {
  const prefix = `SC${yymmdd(now)}`;
  const r = await new sql.Request(tx)
    .input("likePrefix", sql.VarChar(20), `${prefix}%`)
    .query(`
      SELECT TOP 1 op_sc_id
      FROM ${SAFE_OPSCAN} WITH (UPDLOCK, HOLDLOCK)
      WHERE op_sc_id LIKE @likePrefix
      ORDER BY op_sc_id DESC
    `);

  let running = 1;
  if (r.recordset?.length) {
    const lastId = String(r.recordset[0].op_sc_id || "");
    const tail = lastId.slice(prefix.length);
    const n = parseInt(tail, 10);
    if (!Number.isNaN(n)) running = n + 1;
  }
  return `${prefix}${pad(running, 4)}`;
}

function buildResultString(goodQty, scrapQty) {
  const g = Number(goodQty || 0);
  const s = Number(scrapQty || 0);
  if (g > 0 && s > 0) return "OK, NG";
  if (g > 0) return "OK";
  if (s > 0) return "NG";
  return null;
}

async function getLotNoByTkId(txOrPool, tk_id) {
  const req =
    txOrPool instanceof sql.Transaction ? new sql.Request(txOrPool) : txOrPool.request();
  const r = await req
    .input("tk_id", sql.VarChar(20), tk_id)
    .query(`
      SELECT TOP 1 lot_no
      FROM ${SAFE_TKDETAIL} WITH (NOLOCK)
      WHERE tk_id = @tk_id
      ORDER BY tk_created_at_ts DESC
    `);

  return r.recordset?.[0]?.lot_no ?? null;
}

function normalizeSplits(body) {
  if (Array.isArray(body?.splits) && body.splits.length > 0) {
    return body.splits.map((x) => ({
      out_part_no: String(x?.out_part_no || "").trim(),
      qty: Number(x?.qty),
    }));
  }
  return [];
}

async function getPartByNo(tx, part_no) {
  const r = await new sql.Request(tx)
    .input("part_no", sql.VarChar(100), String(part_no).trim())
    .query(`
      SELECT TOP 1 part_id, part_no, part_name
      FROM ${SAFE_PART} WITH (NOLOCK)
      WHERE part_no = @part_no
    `);
  return r.recordset?.[0] ?? null;
}

async function genTransferId(tx, now) {
  // ใช้รูปแบบ TFyymmdd#### เหมือนเดิม
  const prefix = `TF${yymmdd(now)}`;
  const r = await new sql.Request(tx)
    .input("likePrefix", sql.VarChar(20), `${prefix}%`)
    .query(`
      SELECT TOP 1 transfer_id
      FROM ${SAFE_TRANSFER} WITH (UPDLOCK, HOLDLOCK)
      WHERE transfer_id LIKE @likePrefix
      ORDER BY transfer_id DESC
    `);

  let running = 1;
  if (r.recordset?.length) {
    const lastId = String(r.recordset[0].transfer_id || "");
    const tail = lastId.slice(prefix.length);
    const n = parseInt(tail, 10);
    if (!Number.isNaN(n)) running = n + 1;
  }
  return `${prefix}${pad(running, 4)}`;
}

exports.listAllActiveOpScans = async (req, res) => {
  // ✅ ต้องมี actor ก่อน
  const actor =
    (typeof actorOf === "function" && actorOf(req)) ||
    {
      u_id: req.user?.u_id,
      u_name: req.user?.u_name,
      role: req.user?.role,
      u_type: req.user?.u_type,
      op_sta_id: req.user?.op_sta_id,
      op_sta_name: req.user?.op_sta_name,
      clientType: req.user?.clientType,
    };

  const isOperator = actor.role === "operator" || actor.u_type === "op";
  const opSta = actor.op_sta_id ? String(actor.op_sta_id).trim() : "";

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
      LEFT JOIN dbo.machine m ON m.MC_id = s.MC_id
      LEFT JOIN dbo.op_station st
        ON st.op_sta_id = COALESCE(s.op_sta_id, m.op_sta_id)
      OUTER APPLY (
        SELECT TOP 1 d.lot_no
        FROM ${SAFE_TKDETAIL} d WITH (NOLOCK)
        WHERE d.tk_id = s.tk_id
        ORDER BY d.tk_created_at_ts DESC
      ) td
      WHERE s.op_sc_ts IS NOT NULL
        AND s.op_sc_finish_ts IS NULL
        ${isOperator ? "AND COALESCE(LTRIM(RTRIM(s.op_sta_id)), LTRIM(RTRIM(m.op_sta_id))) = @op_sta_id" : ""}
      ORDER BY s.op_sc_ts DESC
    `);

    return res.json({
      actor,
      active: true,
      count: r.recordset.length,
      items: r.recordset,
    });
  } catch (err) {
    console.error("[OPSCAN_ACTIVE_ALL][ERROR]", err);
    return res.status(500).json({ message: "Get active list failed", actor, error: err.message });
  }
};

exports.getOpScanById = async (req, res) => {
  const actor = actorOf(req);
  const op_sc_id = String(req.params.op_sc_id || req.params.id || "").trim();
  if (!op_sc_id) return res.status(400).json({ message: "op_sc_id is required", actor });

  try {
    const pool = await getPool();
    const r = await pool
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


exports.getActiveOpScanByTkId = async (req, res) => {
  const isOperator = actor.role === "operator";
const opSta = actor.op_sta_id ? String(actor.op_sta_id).trim() : "";
if (isOperator && !opSta) return res.status(400).json({ message: "Missing op_sta_id in token", actor });
  try {
    const rq = pool.request().input("tk_id", sql.VarChar(20), tk_id);
if (isOperator) rq.input("op_sta_id", sql.VarChar(20), opSta);

const r = await rq.query(`
  SELECT TOP 1
    s.op_sc_id, s.tk_id, s.MC_id, s.u_id,
    s.op_sta_id,
    s.op_sc_total_qty, s.op_sc_scrap_qty, s.op_sc_good_qty,
    s.tf_rs_code,
    lot_latest.lot_no AS lot_no,
    s.op_sc_ts, s.op_sc_finish_ts
  FROM ${SAFE_OPSCAN} s WITH (NOLOCK)
  LEFT JOIN dbo.machine m ON m.MC_id = s.MC_id
  OUTER APPLY (
    SELECT TOP 1 d.lot_no
    FROM ${SAFE_TKDETAIL} d WITH (NOLOCK)
    WHERE d.tk_id = s.tk_id
    ORDER BY d.tk_created_at_ts DESC
  ) lot_latest
  WHERE s.tk_id = @tk_id
    AND s.op_sc_finish_ts IS NULL
    ${isOperator ? "AND COALESCE(LTRIM(RTRIM(s.op_sta_id)), LTRIM(RTRIM(m.op_sta_id))) = @op_sta_id" : ""}
  ORDER BY s.op_sc_ts DESC
`);

    const row = r.recordset?.[0];
    if (!row) return res.json({ actor, tk_id, active: false, item: null });

    return res.json({ actor, tk_id, active: true, item: row });
  } catch (err) {
    console.error("[OPSCAN_ACTIVE][ERROR]", err);
    return res.status(500).json({ message: "Get active failed", actor, error: err.message });
  }
};

exports.getTkSummary = async (req, res) => {
  const actor = actorOf(req);
  const tk_id = String(req.params.tk_id || "").trim();
  if (!tk_id) return res.status(400).json({ message: "tk_id is required", actor });

  try {
    const pool = await getPool();

    // 1) TKHead — สถานะปัจจุบันของถาด
    const headR = await pool.request()
      .input("tk_id", sql.VarChar(20), tk_id)
      .query(`
        SELECT TOP 1
          h.tk_id,
          h.tk_status,
          h.created_by_u_id,
          h.tk_created_at_ts
        FROM dbo.TKHead h WITH (NOLOCK)
        WHERE h.tk_id = @tk_id
      `);

    const head = headR.recordset?.[0];
    if (!head) return res.status(404).json({ message: "tk_id not found", actor, tk_id });

    // 2) TKDetail — ข้อมูลชิ้นส่วนปัจจุบัน
    const detailR = await pool.request()
      .input("tk_id", sql.VarChar(20), tk_id)
      .query(`
        SELECT TOP 1
          d.tk_id,
          d.lot_no,
          d.part_id,
          p.part_no,
          p.part_name,
          d.MC_id,
          d.op_sta_id,
          d.tk_created_at_ts
        FROM ${SAFE_TKDETAIL} d WITH (NOLOCK)
        LEFT JOIN dbo.part p ON p.part_id = d.part_id
        WHERE d.tk_id = @tk_id
        ORDER BY d.tk_created_at_ts DESC
      `);

    const detail = detailR.recordset?.[0] ?? null;

    // 3) op_scan history — ประวัติการ scan ทุกครั้ง
    const scanR = await pool.request()
      .input("tk_id", sql.VarChar(20), tk_id)
      .query(`
        SELECT
          s.op_sc_id,
          s.op_sta_id,
          st.op_sta_name,
          s.MC_id,
          m.MC_name,
          s.u_id,
          u.u_name,
          s.op_sc_total_qty,
          s.op_sc_good_qty,
          s.op_sc_scrap_qty,
          s.tf_rs_code,
          s.lot_no,
          s.op_sc_ts,
          s.op_sc_finish_ts,
          CASE
            WHEN s.op_sc_finish_ts IS NULL THEN 'IN_PROGRESS'
            ELSE 'DONE'
          END AS scan_status
        FROM ${SAFE_OPSCAN} s WITH (NOLOCK)
        LEFT JOIN dbo.op_station st ON st.op_sta_id = s.op_sta_id
        LEFT JOIN dbo.machine    m  ON m.MC_id      = s.MC_id
        LEFT JOIN dbo.[user]     u  ON u.u_id       = s.u_id
        WHERE s.tk_id = @tk_id
        ORDER BY s.op_sc_ts ASC
      `);

    // 4) transfer history — ประวัติการโอน lot
    const transferR = await pool.request()
      .input("tk_id", sql.VarChar(20), tk_id)
      .query(`
        SELECT
          t.transfer_id,
          t.from_lot_no,
          t.to_lot_no,
          t.tf_rs_code,
          t.transfer_qty,
          t.op_sc_id,
          t.MC_id,
          m.MC_name,
          t.created_by_u_id,
          u.u_name AS created_by_u_name,
          t.transfer_ts
        FROM dbo.t_transfer t WITH (NOLOCK)
        LEFT JOIN dbo.machine m ON m.MC_id   = t.MC_id
        LEFT JOIN dbo.[user]  u ON u.u_id    = t.created_by_u_id
        WHERE t.from_tk_id = @tk_id
           OR t.to_tk_id   = @tk_id
        ORDER BY t.transfer_ts ASC
      `);

    // 5) คำนวณ summary qty
    const scans       = scanR.recordset   || [];
    const transfers   = transferR.recordset || [];
    const totalGood   = scans.reduce((acc, s) => acc + (s.op_sc_good_qty  || 0), 0);
    const totalScrap  = scans.reduce((acc, s) => acc + (s.op_sc_scrap_qty || 0), 0);
    const stationsDone = [...new Set(
      scans
        .filter(s => s.op_sc_finish_ts)
        .map(s => s.op_sta_id)
        .filter(Boolean)
    )];

    const tk_status_label = {
      0: "NOT_STARTED",
      1: "FINISHED",
      2: "PARTIAL_DONE",
      3: "IN_PROGRESS",
    }[head.tk_status] ?? "UNKNOWN";

    console.log(`[TK_SUMMARY] tk_id=${tk_id} tk_status=${head.tk_status} scans=${scans.length} transfers=${transfers.length}`);

    return res.json({
      actor,

      // ข้อมูลถาด
      tk_id:            head.tk_id,
      tk_status:        head.tk_status,
      tk_status_label,
      is_finished:      head.tk_status === 1,
      tk_created_at_ts: head.tk_created_at_ts
        ? new Date(head.tk_created_at_ts).toISOString()
        : null,

      // ชิ้นส่วนปัจจุบัน
      current: detail ? {
        lot_no:    detail.lot_no,
        part_id:   detail.part_id,
        part_no:   detail.part_no,
        part_name: detail.part_name,
        MC_id:     detail.MC_id,
        op_sta_id: detail.op_sta_id,
      } : null,

      // สรุปรวม
      summary: {
        total_scans:   scans.length,
        total_good:    totalGood,
        total_scrap:   totalScrap,
        stations_done: stationsDone,
      },

      // ประวัติ scan ทุกครั้ง
      scans,

      // ประวัติการโอน lot
      transfers,
    });
  } catch (err) {
    console.error("[TK_SUMMARY][ERROR]", err);
    return res.status(500).json({ message: "Get summary failed", actor, error: err.message });
  }
};

exports.startOpScan = async (req, res) => {
const actor = actorOf(req);              
const op_sta_id = actor.op_sta_id;

  if (actor.u_type !== "op") return forbid(res, "Forbidden: u_type must be op", actor);
  if (actor.clientType !== "HH") return forbid(res, "Forbidden: clientType must be HH", actor);
  if (!actor.u_id) return res.status(401).json({ message: "Unauthorized", actor });

  const tk_id = String(req.body.tk_id || "").trim();
  const MC_id = String(req.body.MC_id || "").trim();

  if (!tk_id) return res.status(400).json({ message: "tk_id is required", actor });
  if (!MC_id) return res.status(400).json({ message: "MC_id is required", actor });

  console.log(
    `[OPSCAN_START][REQ] u_id=${actor.u_id} u_name=${actor.u_name} op_sta_id=${actor.op_sta_id ?? "-"} tk_id=${tk_id} MC_id=${MC_id}`
  );

  try {
    const pool = await getPool();
    const tx = new sql.Transaction(pool);
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    try {
      const now = new Date();

      // 1) lock tkdetail + ดึง lot_no ปัจจุบัน
     // 1) lock tkdetail + ดึง lot_no ปัจจุบัน
const tkDetailR = await new sql.Request(tx)
  .input("tk_id", sql.VarChar(20), tk_id)
  .query(`
    SELECT TOP 1
      d.tk_id,
      d.part_id,
      p.part_no,
      p.part_name,
      d.lot_no,
      d.tk_status,
      d.tk_created_at_ts
    FROM ${SAFE_TKDETAIL} d WITH (UPDLOCK, HOLDLOCK)
    LEFT JOIN dbo.part p ON p.part_id = d.part_id
    WHERE d.tk_id = @tk_id
    ORDER BY d.tk_created_at_ts DESC
  `);

const tkDoc = tkDetailR.recordset?.[0];
if (!tkDoc) {
  await tx.rollback();
  return res.status(404).json({ message: "tk_id not found", actor, tk_id });
}

// ✅ เพิ่ม — ดึง lot ทั้งหมดที่ active จาก TKRunLog
const allLotsR = await new sql.Request(tx)
  .input("tk_id", sql.VarChar(20), tk_id)
  .query(`
    SELECT
      r.run_no,
      r.lot_no,
      p.part_no,
      p.part_name,
      r.created_at_ts
    FROM dbo.TKRunLog r WITH (NOLOCK)
    LEFT JOIN dbo.part p ON p.part_id = r.part_id
    WHERE r.tk_id = @tk_id
    ORDER BY r.created_at_ts DESC
  `);

const allLots = allLotsR.recordset || [];
      // 2) เช็ค STA007 finished
      const finishedR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .query(`
          SELECT TOP 1 op_sc_id
          FROM ${SAFE_OPSCAN} WITH (NOLOCK)
          WHERE tk_id             = @tk_id
            AND op_sta_id         = 'STA007'
            AND op_sc_finish_ts IS NOT NULL
        `);

      if (finishedR.recordset?.[0]) {
        await tx.rollback();
        return res.status(403).json({
          message: "This tk_id is already FINISHED at STA007. Cannot start new scan.",
          actor,
          tk_id,
        });
      }

        //ห้ามย้อน STATION เดิมที่เคย finish ไปแล้วจ้า
      const lastFinishedStaR = await new sql.Request(tx)
  .input("tk_id", sql.VarChar(20), tk_id)
  .query(`
    SELECT TOP 1 
      s.op_sta_id,
      st.op_sta_name        -- ✅ เพิ่ม
    FROM ${SAFE_OPSCAN} s WITH (NOLOCK)
    LEFT JOIN dbo.op_station st ON st.op_sta_id = s.op_sta_id
    WHERE s.tk_id             = @tk_id
      AND s.op_sc_finish_ts IS NOT NULL
      AND s.op_sta_id        IS NOT NULL
    ORDER BY s.op_sc_finish_ts DESC
  `);

const lastFinishedSta     = lastFinishedStaR.recordset?.[0]?.op_sta_id   ?? null;
const lastFinishedStaName = lastFinishedStaR.recordset?.[0]?.op_sta_name ?? null;

if (lastFinishedSta) {
  const staNum     = (sta) => parseInt(String(sta).replace("STA", ""), 10);
  const lastNum    = staNum(lastFinishedSta);
  const currentNum = staNum(actor.op_sta_id);

  if (currentNum <= lastNum) {

    // ✅ query หา station ถัดไปจาก op_station
    const nextStaR = await new sql.Request(tx)
      .input("lastNum", sql.Int, lastNum)
      .query(`
        SELECT TOP 1
          op_sta_id,
          op_sta_name
        FROM dbo.op_station
        WHERE CAST(REPLACE(op_sta_id, 'STA', '') AS INT) > @lastNum
          AND op_sta_active = 1
        ORDER BY CAST(REPLACE(op_sta_id, 'STA', '') AS INT) ASC
      `);

    const nextSta = nextStaR.recordset?.[0] ?? null;

    await tx.rollback();
    return res.status(403).json({
      message: nextSta
        ? `tk_id นี้ผ่าน ${lastFinishedSta} (${lastFinishedStaName}) มาแล้ว กรุณานำถาดไปดำเนินการที่ ${nextSta.op_sta_id} (${nextSta.op_sta_name})`
        : `tk_id นี้ผ่าน ${lastFinishedSta} (${lastFinishedStaName}) มาแล้ว ไม่มี Station ถัดไป`,
      actor,
      tk_id,
      last_finished_sta:      lastFinishedSta,
      last_finished_sta_name: lastFinishedStaName,
      next_sta:               nextSta?.op_sta_id   ?? null,
      next_sta_name:          nextSta?.op_sta_name ?? null,
    });
  }
}

      const lot_no = tkDoc.lot_no || null;

      // 4) validate machine
      const mcR = await new sql.Request(tx)
        .input("MC_id",     sql.VarChar(10), MC_id)
        .input("op_sta_id", sql.VarChar(20), actor.op_sta_id ?? null)
        .query(`
          SELECT TOP 1 MC_id, MC_name, op_sta_id
          FROM ${SAFE_MACHINE} WITH (NOLOCK)
          WHERE MC_id     = @MC_id
            AND MC_active = 1
        `);

      const mcRow = mcR.recordset?.[0];

      if (!mcRow) {
        await tx.rollback();
        return res.status(400).json({ message: "MC_id not found or inactive", actor, MC_id });
      }

      if (mcRow.op_sta_id !== actor.op_sta_id) {
        await tx.rollback();
        return res.status(403).json({
          message:           `Machine ${MC_id} does not belong to your station (${actor.op_sta_id}). Machine is in ${mcRow.op_sta_id ?? "no station"}.`,
          actor,
          MC_id,
          machine_op_sta_id: mcRow.op_sta_id ?? null,
          your_op_sta_id:    actor.op_sta_id ?? null,
        });
      }

      // 5) เช็คงานค้าง (active scan)
      const activeR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .query(`
          SELECT TOP 1 op_sc_id
          FROM ${SAFE_OPSCAN} WITH (UPDLOCK, HOLDLOCK)
          WHERE tk_id             = @tk_id
            AND op_sc_finish_ts IS NULL
          ORDER BY op_sc_ts DESC
        `);

      if (activeR.recordset?.[0]) {
        await tx.rollback();
        return res.status(409).json({
          message:  "This tk_id already has an active scan (not finished yet)",
          actor,
          tk_id,
          op_sc_id: activeR.recordset[0].op_sc_id,
        });
      }

      // 6) gen op_sc_id ✅ ต้องก่อน UPDATE TKDetail
      const op_sc_id = await genOpScId(tx, now);

      // 7) update TKDetail ✅ ใช้ op_sc_id ที่ gen แล้ว
      await new sql.Request(tx)
        .input("tk_id",    sql.VarChar(20), tk_id)
        .input("MC_id",    sql.VarChar(10), MC_id)
        .input("op_sta_id",sql.VarChar(20), actor.op_sta_id ?? null)
        .input("op_sc_id", sql.Char(12),    op_sc_id)
        .query(`
          UPDATE ${SAFE_TKDETAIL}
          SET
            MC_id     = @MC_id,
            op_sta_id = @op_sta_id,
            op_sc_id  = @op_sc_id
          WHERE tk_id = @tk_id
        `);

      // 8) INSERT op_scan
      await new sql.Request(tx)
  .input("op_sc_id", sql.Char(12), op_sc_id)
  .input("tk_id", sql.VarChar(20), tk_id)
  .input("op_sta_id", sql.VarChar(20), op_sta_id)   // ✅ เพิ่ม
  .input("MC_id", sql.VarChar(10), MC_id)
  .input("u_id", sql.Int, Number(actor.u_id))
  .input("lot_no", sql.NVarChar(300), lot_no)
  .input("op_sc_ts", sql.DateTime2(3), now)
  .query(`
    INSERT INTO ${SAFE_OPSCAN}
      (op_sc_id, tk_id, op_sta_id, MC_id, u_id,
       op_sc_total_qty, op_sc_scrap_qty, op_sc_good_qty,
       tf_rs_code,
       lot_no,
       op_sc_ts, op_sc_finish_ts)
    VALUES
      (@op_sc_id, @tk_id, @op_sta_id, @MC_id, @u_id,
       0, 0, 0,
       NULL,
       @lot_no,
       @op_sc_ts, NULL)
  `);

     // 9) update TKHead tk_status = 3 (IN_PROGRESS)
await new sql.Request(tx)
  .input("tk_id", sql.VarChar(20), tk_id)
  .query(`
    UPDATE dbo.TKHead
    SET tk_status = 3
    WHERE tk_id     = @tk_id
      AND tk_status = 0
  `);

// ✅ sync tk_status = 3 ลง TKDetail ด้วย
await new sql.Request(tx)
  .input("tk_id", sql.VarChar(20), tk_id)
  .query(`
    UPDATE ${SAFE_TKDETAIL}
    SET tk_status = 3
    WHERE tk_id     = @tk_id
      AND tk_status = 0
  `);
      await tx.commit();

      console.log(
        `[OPSCAN_START][OK] op_sc_id=${op_sc_id} tk_id=${tk_id} op_sta_id=${actor.op_sta_id ?? "-"} MC_id=${MC_id} lot_no=${lot_no ?? "-"} u_id=${actor.u_id}`
      );

      return res.status(201).json({
        message:         "Started",
        actor:           { u_id: actor.u_id, u_name: actor.u_name, role: actor.role },
        op_sc_id,
        op_sta_id:       actor.op_sta_id  ?? null,
        op_sta_name:     actor.op_sta_name ?? null,
        MC_id,
        op_sc_total_qty: 0,
        tk_doc: {
          tk_id:            tkDoc.tk_id,
          lot_no:           tkDoc.lot_no,
          part_id:          tkDoc.part_id,
          part_no:          tkDoc.part_no,
          part_name:        tkDoc.part_name,
          op_sta_id:        actor.op_sta_id  ?? null,
          op_sta_name:      actor.op_sta_name ?? null,
          tk_status:        3,
          tk_created_at_ts: tkDoc.tk_created_at_ts
            ? new Date(tkDoc.tk_created_at_ts).toISOString()
            : null,
        },
        current_lots: allLots.map(l => ({
    run_no:    l.run_no ? String(l.run_no).trim() : null,
    lot_no:    l.lot_no,
    part_no:   l.part_no,
    part_name: l.part_name,
  })),

      });
    } catch (e) {
      await tx.rollback();
      throw e;
    }
  } catch (err) {
    console.error("[OPSCAN_START][ERROR]", err);
    return res.status(500).json({ message: "Start failed", actor, error: err.message });
  }
};

exports.finishOpScan = async (req, res) => {
  const actor = actorOf(req);

  if (actor.u_type !== "op") return forbid(res, "Forbidden: u_type must be op", actor);
  if (actor.clientType !== "HH") return forbid(res, "Forbidden: clientType must be HH", actor);
  if (!actor.u_id) return res.status(401).json({ message: "Unauthorized", actor });

  const op_sc_id      = String(req.body.op_sc_id || "").trim();
  const good_qty_raw  = Number(req.body.good_qty);
  const scrap_qty_raw = Number(req.body.scrap_qty);
  const groups        = Array.isArray(req.body.groups) ? req.body.groups : [];

  if (!op_sc_id) return res.status(400).json({ message: "op_sc_id is required", actor });
  if (![good_qty_raw, scrap_qty_raw].every(Number.isFinite)) {
    return res.status(400).json({ message: "good_qty/scrap_qty must be numbers", actor });
  }

  const good_qty  = Math.abs(good_qty_raw);
  const scrap_qty = Math.abs(scrap_qty_raw);
  const total_qty = good_qty + scrap_qty;

  if (good_qty === 0 && scrap_qty === 0) {
    return res.status(400).json({ message: "good_qty and scrap_qty cannot both be 0", actor });
  }
  if (groups.length === 0) {
    return res.status(400).json({ message: "groups[] is required and cannot be empty", actor });
  }

  // --- validate แต่ละ group ---
  for (let i = 0; i < groups.length; i++) {
    const g    = groups[i];
    const gNum = i + 1;
    const tf   = Number(g.tf_rs_code);

    if (![1, 2, 3].includes(tf)) {
      return res.status(400).json({ message: `groups[${gNum}]: tf_rs_code must be 1, 2, or 3`, actor });
    }
    if (!Number.isFinite(Number(g.qty)) || Number(g.qty) <= 0) {
      return res.status(400).json({ message: `groups[${gNum}]: qty must be > 0`, actor });
    }

    if (tf === 1) {
      if (!g.out_part_no || !String(g.out_part_no).trim()) {
        return res.status(400).json({ message: `groups[${gNum}]: out_part_no is required`, actor });
      }
    }

    if (tf === 2) {
      if (!Array.isArray(g.splits) || g.splits.length < 2) {
        return res.status(400).json({ message: `groups[${gNum}]: splits[] must have >= 2 items`, actor });
      }
      for (const s of g.splits) {
        if (!s.out_part_no || !String(s.out_part_no).trim()) {
          return res.status(400).json({ message: `groups[${gNum}]: every split must have out_part_no`, actor });
        }
        if (!Number.isFinite(Number(s.qty)) || Number(s.qty) <= 0) {
          return res.status(400).json({ message: `groups[${gNum}]: split qty must be > 0`, actor });
        }
      }
      const sumSplits = g.splits.reduce((acc, s) => acc + Math.trunc(Number(s.qty)), 0);
      if (sumSplits !== Math.trunc(Number(g.qty))) {
        return res.status(400).json({
          message: `groups[${gNum}]: sum of splits qty (${sumSplits}) must equal group qty (${Math.trunc(Number(g.qty))})`,
          actor,
        });
      }
    }

    if (tf === 3) {
      if (!g.out_part_no || !String(g.out_part_no).trim()) {
        return res.status(400).json({ message: `groups[${gNum}]: out_part_no is required`, actor });
      }
      if (!Array.isArray(g.merge_lots) || g.merge_lots.length < 2) {
        return res.status(400).json({ message: `groups[${gNum}]: merge_lots[] must have >= 2 lots`, actor });
      }
      for (const m of g.merge_lots) {
        if (!m.from_lot_no || !String(m.from_lot_no).trim()) {
          return res.status(400).json({ message: `groups[${gNum}]: every merge_lot must have from_lot_no`, actor });
        }
        if (!Number.isFinite(Number(m.qty)) || Number(m.qty) <= 0) {
          return res.status(400).json({ message: `groups[${gNum}]: merge_lot qty must be > 0`, actor });
        }
      }
      const sumMerge = g.merge_lots.reduce((acc, m) => acc + Math.trunc(Number(m.qty)), 0);
      if (sumMerge !== Math.trunc(Number(g.qty))) {
        return res.status(400).json({
          message: `groups[${gNum}]: sum of merge_lots qty (${sumMerge}) must equal group qty (${Math.trunc(Number(g.qty))})`,
          actor,
        });
      }
    }
  }

  // sum(groups[].qty) ต้องเท่ากับ good_qty
  const sumGroupsQty = groups.reduce((acc, g) => acc + Math.trunc(Number(g.qty)), 0);
  if (sumGroupsQty !== Math.trunc(good_qty)) {
    return res.status(400).json({
      message:        `Sum of all groups qty (${sumGroupsQty}) must equal good_qty (${Math.trunc(good_qty)})`,
      actor,
      good_qty:       Math.trunc(good_qty),
      sum_groups_qty: sumGroupsQty,
    });
  }

  // กัน from_lot_no ซ้ำข้ามกลุ่ม
  const allFromLots = [];
  for (const g of groups) {
    const tf = Number(g.tf_rs_code);
    if (tf === 2 && g.from_lot_no) {
      const lot = String(g.from_lot_no).trim();
      if (allFromLots.includes(lot)) {
        return res.status(400).json({ message: `from_lot_no "${lot}" is used more than once`, actor });
      }
      allFromLots.push(lot);
    }
    if (tf === 3) {
      for (const m of g.merge_lots) {
        const lot = String(m.from_lot_no).trim();
        if (allFromLots.includes(lot)) {
          return res.status(400).json({ message: `from_lot_no "${lot}" is used more than once`, actor });
        }
        allFromLots.push(lot);
      }
    }
  }

  const SAFE_TRANSFER = safeTableName(process.env.TRANSFER_TABLE || "dbo.t_transfer");
  const SAFE_PART     = safeTableName(process.env.PART_TABLE     || "dbo.part");
  const SAFE_RUNLOG   = safeTableName(process.env.TKRUNLOG_TABLE || "dbo.TKRunLog");

  console.log(
    `[OPSCAN_FINISH][REQ] u_id=${actor.u_id} op_sc_id=${op_sc_id} good=${Math.trunc(good_qty)} scrap=${Math.trunc(scrap_qty)} groups=${groups.length}`
  );

  try {
    const pool = await getPool();
    const tx   = new sql.Transaction(pool);
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    const getPartByNo = async (partNo) => {
      const r = await new sql.Request(tx)
        .input("part_no", sql.VarChar(100), String(partNo).trim())
        .query(`
          SELECT TOP 1 part_id, part_no, part_name
          FROM ${SAFE_PART} WITH (NOLOCK)
          WHERE part_no = @part_no
        `);
      return r.recordset?.[0] ?? null;
    };

    const lotExistsInRunLog = async (tkId, lotNo) => {
      const r = await new sql.Request(tx)
        .input("tk_id",  sql.VarChar(20),  tkId)
        .input("lot_no", sql.NVarChar(300), lotNo)
        .query(`
          SELECT TOP 1 lot_no
          FROM ${SAFE_RUNLOG} WITH (NOLOCK)
          WHERE tk_id = @tk_id AND lot_no = @lot_no
        `);
      return !!r.recordset?.[0];
    };

    const genNewLot = async (tkId, partId) => {
      const sp = await new sql.Request(tx)
        .input("tk_id",           sql.VarChar(20), tkId)
        .input("part_id",         sql.Int,         Number(partId))
        .input("created_by_u_id", sql.Int,         Number(actor.u_id))
        .output("run_no",         sql.Char(14))
        .output("lot_no",         sql.NVarChar(300))
        .execute("dbo.usp_TKRunLog_Create");

      const run_no = sp.output.run_no;
      const lot_no = sp.output.lot_no;
      if (!run_no || !lot_no) throw new Error("DB did not return run_no/lot_no");
      return { run_no: String(run_no).trim(), lot_no: String(lot_no) };
    };

    try {
      const now = new Date();

      // 1) lock op_scan row
      const rowR = await new sql.Request(tx)
        .input("op_sc_id", sql.Char(12), op_sc_id)
        .query(`
          SELECT TOP 1 *
          FROM ${SAFE_OPSCAN} WITH (UPDLOCK, HOLDLOCK)
          WHERE op_sc_id = @op_sc_id
        `);

      const row = rowR.recordset?.[0];
      if (!row) {
        await tx.rollback();
        return res.status(404).json({ message: "op_sc_id not found", actor, op_sc_id });
      }
      if (row.op_sc_finish_ts) {
        await tx.rollback();
        return res.status(409).json({ message: "Already finished", actor, op_sc_id });
      }

      const master_tk_id = String(row.tk_id || "").trim();
      if (!master_tk_id) {
        await tx.rollback();
        return res.status(400).json({ message: "op_scan.tk_id is NULL", actor, op_sc_id });
      }

      // 2) เช็ค STA007 finished
      const finishedR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), master_tk_id)
        .query(`
          SELECT TOP 1 op_sc_id
          FROM ${SAFE_OPSCAN} WITH (NOLOCK)
          WHERE tk_id             = @tk_id
            AND op_sta_id         = 'STA007'
            AND op_sc_finish_ts IS NOT NULL
        `);
      if (finishedR.recordset?.[0]) {
        await tx.rollback();
        return res.status(403).json({
          message: "This tk_id is already FINISHED at STA007. Cannot finish again.",
          actor, tk_id: master_tk_id,
        });
      }

      // 3) ดึง good_qty จาก station ก่อนหน้า
      const prevScanR = await new sql.Request(tx)
        .input("tk_id",    sql.VarChar(20), master_tk_id)
        .input("op_sc_id", sql.Char(12),    op_sc_id)
        .query(`
          SELECT TOP 1 op_sc_good_qty
          FROM ${SAFE_OPSCAN} WITH (NOLOCK)
          WHERE tk_id             = @tk_id
            AND op_sc_finish_ts IS NOT NULL
            AND op_sc_id         <> @op_sc_id
          ORDER BY op_sc_finish_ts DESC
        `);

      const prevGoodQty = prevScanR.recordset?.[0]?.op_sc_good_qty ?? null;

      if (prevGoodQty !== null && sumGroupsQty !== Math.trunc(prevGoodQty)) {
        await tx.rollback();
        return res.status(400).json({
          message:        `Sum of groups qty (${sumGroupsQty}) must equal previous station good_qty (${Math.trunc(prevGoodQty)})`,
          actor,
          sum_groups_qty: sumGroupsQty,
          prev_good_qty:  Math.trunc(prevGoodQty),
        });
      }

      // 4) ดึง base_lot_no จาก TKDetail (กรณีไม่มี lot เดิม)
      const baseDetailR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), master_tk_id)
        .query(`
          SELECT TOP 1 lot_no, part_id
          FROM ${SAFE_TKDETAIL} WITH (NOLOCK)
          WHERE tk_id = @tk_id
          ORDER BY tk_created_at_ts DESC
        `);

      const baseDetail  = baseDetailR.recordset?.[0];
      const base_lot_no = baseDetail?.lot_no ? String(baseDetail.lot_no).trim() : null;

      // 5) validate from_lot_no ทุกตัว
      for (const g of groups) {
        const tf = Number(g.tf_rs_code);
        if (tf === 2 && g.from_lot_no) {
          const lot = String(g.from_lot_no).trim();
          const ok  = await lotExistsInRunLog(master_tk_id, lot);
          if (!ok) {
            await tx.rollback();
            return res.status(400).json({
              message: `from_lot_no "${lot}" not found in TKRunLog for tk_id ${master_tk_id}`,
              actor,
            });
          }
        }
        if (tf === 3) {
          for (const m of g.merge_lots) {
            const lot = String(m.from_lot_no).trim();
            const ok  = await lotExistsInRunLog(master_tk_id, lot);
            if (!ok) {
              await tx.rollback();
              return res.status(400).json({
                message: `from_lot_no "${lot}" not found in TKRunLog for tk_id ${master_tk_id}`,
                actor,
              });
            }
          }
        }
      }

      // 6) ประมวลผลแต่ละ group
      const created_children = [];
      let   first_lot_no     = null;

      for (let i = 0; i < groups.length; i++) {
        const g         = groups[i];
        const tf        = Number(g.tf_rs_code);
        const group_qty = Math.trunc(Number(g.qty));
        const gNum      = i + 1;

        // ----------------------------------------------------------------
        // tf=1 Master → 1 เข้า 1 ออก
        // ----------------------------------------------------------------
        if (tf === 1) {
          const outPart  = await getPartByNo(String(g.out_part_no).trim());
          if (!outPart) {
            await tx.rollback();
            return res.status(400).json({ message: `groups[${gNum}]: out_part_no "${g.out_part_no}" not found`, actor });
          }

          const from_lot           = g.from_lot_no ? String(g.from_lot_no).trim() : base_lot_no;
          const { run_no, lot_no } = await genNewLot(master_tk_id, outPart.part_id);

          if (!first_lot_no) first_lot_no = lot_no;

          await new sql.Request(tx)
            .input("tk_id",   sql.VarChar(20),  master_tk_id)
            .input("part_id", sql.Int,           Number(outPart.part_id))
            .input("lot_no",  sql.NVarChar(300), lot_no)
            .query(`UPDATE ${SAFE_TKDETAIL} SET part_id=@part_id, lot_no=@lot_no WHERE tk_id=@tk_id`);

          await new sql.Request(tx)
            .input("from_tk_id",      sql.VarChar(20),   master_tk_id)
            .input("to_tk_id",        sql.VarChar(20),   master_tk_id)
            .input("from_lot_no",     sql.NVarChar(300), from_lot || "")
            .input("to_lot_no",       sql.NVarChar(300), lot_no)
            .input("tf_rs_code",      sql.Int,           1)
            .input("transfer_qty",    sql.Int,           group_qty)
            .input("op_sc_id",        sql.Char(12),      op_sc_id)
            .input("MC_id",           sql.VarChar(10),   row.MC_id ? String(row.MC_id).trim() : null)
            .input("created_by_u_id", sql.Int,           Number(actor.u_id))
            .input("transfer_ts",     sql.DateTime2(3),  now)
            .query(`
              INSERT INTO ${SAFE_TRANSFER}
                (from_tk_id, to_tk_id, from_lot_no, to_lot_no,
                 tf_rs_code, transfer_qty, op_sc_id, MC_id, created_by_u_id, transfer_ts)
              VALUES
                (@from_tk_id, @to_tk_id, @from_lot_no, @to_lot_no,
                 @tf_rs_code, @transfer_qty, @op_sc_id, @MC_id, @created_by_u_id, @transfer_ts)
            `);

          created_children.push({
            group: gNum, tf_rs_code: 1,
            from_lot_no: from_lot,
            lots: [{ run_no, lot_no, out_part_no: String(outPart.part_no), qty: group_qty }],
          });

          console.log(`[FINISH][G${gNum}][MASTER] from=${from_lot} to=${lot_no} qty=${group_qty}`);
        }

        // ----------------------------------------------------------------
        // tf=2 Split → 1 เข้า หลายออก
        // ----------------------------------------------------------------
        if (tf === 2) {
          const from_lot  = g.from_lot_no ? String(g.from_lot_no).trim() : base_lot_no;
          const splitLots = [];

          for (const s of g.splits) {
            const outPart = await getPartByNo(String(s.out_part_no).trim());
            if (!outPart) {
              await tx.rollback();
              return res.status(400).json({
                message: `groups[${gNum}] split: out_part_no "${s.out_part_no}" not found`, actor,
              });
            }

            const s_qty              = Math.trunc(Number(s.qty));
            const { run_no, lot_no } = await genNewLot(master_tk_id, outPart.part_id);

            if (!first_lot_no) first_lot_no = lot_no;

            await new sql.Request(tx)
              .input("from_tk_id",      sql.VarChar(20),   master_tk_id)
              .input("to_tk_id",        sql.VarChar(20),   master_tk_id)
              .input("from_lot_no",     sql.NVarChar(300), from_lot || "")
              .input("to_lot_no",       sql.NVarChar(300), lot_no)
              .input("tf_rs_code",      sql.Int,           2)
              .input("transfer_qty",    sql.Int,           s_qty)
              .input("op_sc_id",        sql.Char(12),      op_sc_id)
              .input("MC_id",           sql.VarChar(10),   row.MC_id ? String(row.MC_id).trim() : null)
              .input("created_by_u_id", sql.Int,           Number(actor.u_id))
              .input("transfer_ts",     sql.DateTime2(3),  now)
              .query(`
                INSERT INTO ${SAFE_TRANSFER}
                  (from_tk_id, to_tk_id, from_lot_no, to_lot_no,
                   tf_rs_code, transfer_qty, op_sc_id, MC_id, created_by_u_id, transfer_ts)
                VALUES
                  (@from_tk_id, @to_tk_id, @from_lot_no, @to_lot_no,
                   @tf_rs_code, @transfer_qty, @op_sc_id, @MC_id, @created_by_u_id, @transfer_ts)
              `);

            splitLots.push({ run_no, lot_no, out_part_no: String(outPart.part_no), qty: s_qty });
          }

          created_children.push({
            group: gNum, tf_rs_code: 2,
            from_lot_no: from_lot,
            lots: splitLots,
          });

          console.log(`[FINISH][G${gNum}][SPLIT] from=${from_lot} splits=${g.splits.length} qty=${group_qty}`);
        }

        // ----------------------------------------------------------------
        // tf=3 Co-ID → หลายเข้า 1 ออก
        // ----------------------------------------------------------------
        if (tf === 3) {
          const outPart = await getPartByNo(String(g.out_part_no).trim());
          if (!outPart) {
            await tx.rollback();
            return res.status(400).json({ message: `groups[${gNum}]: out_part_no "${g.out_part_no}" not found`, actor });
          }

          const { run_no, lot_no } = await genNewLot(master_tk_id, outPart.part_id);

          if (!first_lot_no) first_lot_no = lot_no;

          for (const m of g.merge_lots) {
            await new sql.Request(tx)
              .input("from_tk_id",      sql.VarChar(20),   master_tk_id)
              .input("to_tk_id",        sql.VarChar(20),   master_tk_id)
              .input("from_lot_no",     sql.NVarChar(300), String(m.from_lot_no).trim())
              .input("to_lot_no",       sql.NVarChar(300), lot_no)
              .input("tf_rs_code",      sql.Int,           3)
              .input("transfer_qty",    sql.Int,           Math.trunc(Number(m.qty)))
              .input("op_sc_id",        sql.Char(12),      op_sc_id)
              .input("MC_id",           sql.VarChar(10),   row.MC_id ? String(row.MC_id).trim() : null)
              .input("created_by_u_id", sql.Int,           Number(actor.u_id))
              .input("transfer_ts",     sql.DateTime2(3),  now)
              .query(`
                INSERT INTO ${SAFE_TRANSFER}
                  (from_tk_id, to_tk_id, from_lot_no, to_lot_no,
                   tf_rs_code, transfer_qty, op_sc_id, MC_id, created_by_u_id, transfer_ts)
                VALUES
                  (@from_tk_id, @to_tk_id, @from_lot_no, @to_lot_no,
                   @tf_rs_code, @transfer_qty, @op_sc_id, @MC_id, @created_by_u_id, @transfer_ts)
              `);
          }

          await new sql.Request(tx)
            .input("tk_id",   sql.VarChar(20),  master_tk_id)
            .input("part_id", sql.Int,           Number(outPart.part_id))
            .input("lot_no",  sql.NVarChar(300), lot_no)
            .query(`UPDATE ${SAFE_TKDETAIL} SET part_id=@part_id, lot_no=@lot_no WHERE tk_id=@tk_id`);

          created_children.push({
            group: gNum, tf_rs_code: 3,
            out_part_no:   String(outPart.part_no),
            out_part_name: outPart.part_name ?? null,
            merged_from:   g.merge_lots.map(m => ({ from_lot_no: m.from_lot_no, qty: Math.trunc(Number(m.qty)) })),
            lots: [{ run_no, lot_no, qty: group_qty }],
          });

          console.log(`[FINISH][G${gNum}][COID] to_lot=${lot_no} merged=${g.merge_lots.length} qty=${group_qty}`);
        }
      }

      // 7) update op_scan finish
      // ✅ tf_rs_code = group ล่าสุดที่ประมวลผล
      // ✅ lot_no = first_lot_no (lot แรกที่ gen) รายละเอียดทั้งหมดอยู่ใน t_transfer
      const last_tf_rs_code = Number(groups[groups.length - 1].tf_rs_code);

      await new sql.Request(tx)
        .input("op_sc_id",   sql.Char(12),     op_sc_id)
        .input("total_qty",  sql.Int,           Math.trunc(total_qty))
        .input("good_qty",   sql.Int,           Math.trunc(good_qty))
        .input("scrap_qty",  sql.Int,           Math.trunc(scrap_qty))
        .input("lot_no",     sql.NVarChar(300), first_lot_no || "")
        .input("op_sta_id",  sql.VarChar(20),   actor.op_sta_id ?? null)
        .input("finish_ts",  sql.DateTime2(3),  now)
        .input("tf_rs_code", sql.Int,           last_tf_rs_code)
        .query(`
          UPDATE ${SAFE_OPSCAN}
          SET
            op_sc_total_qty = @total_qty,
            op_sc_good_qty  = @good_qty,
            op_sc_scrap_qty = @scrap_qty,
            tf_rs_code      = @tf_rs_code,
            lot_no          = @lot_no,
            op_sta_id       = COALESCE(op_sta_id, @op_sta_id),
            op_sc_finish_ts = @finish_ts
          WHERE op_sc_id = @op_sc_id
        `);

      // 8) update TKHead tk_status
const isFinishAtSTA007 = actor.op_sta_id === "STA007";
const newTkStatus      = isFinishAtSTA007 ? 1 : 2;

await new sql.Request(tx)
  .input("tk_id",     sql.VarChar(20), master_tk_id)
  .input("tk_status", sql.Int,         newTkStatus)
  .query(`
    UPDATE dbo.TKHead
    SET tk_status = @tk_status
    WHERE tk_id = @tk_id
  `);

// ✅ sync tk_status ลง TKDetail ด้วย
await new sql.Request(tx)
  .input("tk_id",     sql.VarChar(20), master_tk_id)
  .input("tk_status", sql.Int,         newTkStatus)
  .query(`
    UPDATE ${SAFE_TKDETAIL}
    SET tk_status = @tk_status
    WHERE tk_id = @tk_id
  `);
      await tx.commit();

      console.log(
        `[OPSCAN_FINISH][OK] op_sc_id=${op_sc_id} tk_id=${master_tk_id} groups=${groups.length} tk_status=${newTkStatus} first_lot=${first_lot_no}`
      );

      return res.json({
        message: "Finished",
        actor:   { u_id: actor.u_id, u_name: actor.u_name, role: actor.role },

        op_sc_id,
        tk_id:           master_tk_id,
        op_sta_id:       actor.op_sta_id  ?? null,
        op_sta_name:     actor.op_sta_name ?? null,
        MC_id:           row.MC_id ?? null,

        op_sc_total_qty: Math.trunc(total_qty),
        op_sc_good_qty:  Math.trunc(good_qty),
        op_sc_scrap_qty: Math.trunc(scrap_qty),

        tk_status:    newTkStatus,
        is_finished:  isFinishAtSTA007,

        created_groups_count: created_children.length,
        created_groups:       created_children,

        op_sc_ts:        row.op_sc_ts ? new Date(row.op_sc_ts).toISOString() : null,
        op_sc_finish_ts: now.toISOString(),
      });

    } catch (e) {
      await tx.rollback();
      throw e;
    }
  } catch (err) {
    console.error("[OPSCAN_FINISH][ERROR]", err);
    return res.status(500).json({ message: "Finish failed", actor, error: err.message });
  }
};