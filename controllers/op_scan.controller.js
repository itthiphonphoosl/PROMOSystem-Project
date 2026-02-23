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
  const actor = actorOf(req);

  try {
    const pool = await getPool();
    const r = await pool.request().query(`
      SELECT TOP (200)
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
        td.lot_no,
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
      ) td
      WHERE s.op_sc_ts IS NOT NULL
        AND s.op_sc_finish_ts IS NULL
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
  const actor = actorOf(req);
  const tk_id = String(req.params.tk_id || req.params.id || "").trim();
  if (!tk_id) return res.status(400).json({ message: "tk_id is required", actor });

  try {
    const pool = await getPool();
    const r = await pool
      .request()
      .input("tk_id", sql.VarChar(20), tk_id)
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
        WHERE s.tk_id = @tk_id
          AND s.op_sc_finish_ts IS NULL
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
// ------------------------------
// START
// ------------------------------
exports.startOpScan = async (req, res) => {
  const actor = actorOf(req);

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

      // 🔴 2) เช็ค is_finished จาก TKHead ก่อน start
      // ถ้ามี op_scan ที่ finish ที่ STA007 แล้ว → reject ทันที
      const finishedR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .query(`
          SELECT TOP 1 op_sc_id
          FROM ${SAFE_OPSCAN} WITH (NOLOCK)
          WHERE tk_id        = @tk_id
            AND op_sta_id    = 'STA007'
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

      const lot_no = tkDoc.lot_no || null;

     // 3) validate machine — ต้องอยู่ใน station เดียวกับที่ login
const mcR = await new sql.Request(tx)
  .input("MC_id",      sql.VarChar(10), MC_id)
  .input("op_sta_id",  sql.VarChar(20), actor.op_sta_id ?? null)
  .query(`
    SELECT TOP 1 MC_id, MC_name, op_sta_id
    FROM ${SAFE_MACHINE} WITH (NOLOCK)
    WHERE MC_id     = @MC_id
      AND MC_active = 1
  `);

const mcRow = mcR.recordset?.[0];

// 3 ไม่มีเครื่องนี้หรือ inactive
if (!mcRow) {
  await tx.rollback();
  return res.status(400).json({
    message: "MC_id not found or inactive",
    actor,
    MC_id,
  });
}

// เครื่องนี้ไม่ได้อยู่ใน station ที่ login
if (mcRow.op_sta_id !== actor.op_sta_id) {
  await tx.rollback();
  return res.status(403).json({
    message: `Machine ${MC_id} does not belong to your station (${actor.op_sta_id}). Machine is in ${mcRow.op_sta_id ?? "no station"}.`,
    actor,
    MC_id,
    machine_op_sta_id: mcRow.op_sta_id ?? null,
    your_op_sta_id:    actor.op_sta_id ?? null,
  });
}

      // 4) เช็คงานค้าง (active scan)
      const activeR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .query(`
          SELECT TOP 1 op_sc_id
          FROM ${SAFE_OPSCAN} WITH (UPDLOCK, HOLDLOCK)
          WHERE tk_id = @tk_id
            AND op_sc_finish_ts IS NULL
          ORDER BY op_sc_ts DESC
        `);

      if (activeR.recordset?.[0]) {
        await tx.rollback();
        return res.status(409).json({
          message: "This tk_id already has an active scan (not finished yet)",
          actor,
          tk_id,
          op_sc_id: activeR.recordset[0].op_sc_id,
        });
      }

      // 5) update TKDetail ให้จำ MC_id + op_sta_id ตอน start
      await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .input("MC_id", sql.VarChar(10), MC_id)
        .input("op_sta_id", sql.VarChar(20), actor.op_sta_id ?? null)
        .query(`
          UPDATE ${SAFE_TKDETAIL}
          SET
            MC_id     = @MC_id,
            op_sta_id = @op_sta_id
          WHERE tk_id = @tk_id
        `);

      // 6) gen op_sc_id
      const op_sc_id = await genOpScId(tx, now);

      // 🔴 7) INSERT op_scan — เพิ่ม op_sta_id
      await new sql.Request(tx)
        .input("op_sc_id",  sql.Char(12),       op_sc_id)
        .input("tk_id",     sql.VarChar(20),     tk_id)
        .input("op_sta_id", sql.VarChar(20),     actor.op_sta_id ?? null)
        .input("MC_id",     sql.VarChar(10),     MC_id)
        .input("u_id",      sql.Int,             Number(actor.u_id))
        .input("lot_no",    sql.NVarChar(300),   lot_no)
        .input("op_sc_ts",  sql.DateTime2(3),    now)
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

      // 🔴 8) update TKHead tk_status = 3 (IN_PROGRESS)
      await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .query(`
          UPDATE dbo.TKHead
          SET tk_status = 3
          WHERE tk_id = @tk_id
            AND tk_status = 0
        `);

      await tx.commit();

      console.log(
        `[OPSCAN_START][OK] op_sc_id=${op_sc_id} tk_id=${tk_id} op_sta_id=${actor.op_sta_id ?? "-"} MC_id=${MC_id} lot_no=${lot_no ?? "-"} u_id=${actor.u_id}`
      );

      return res.status(201).json({
        message: "Started",
        actor: { u_id: actor.u_id, u_name: actor.u_name, role: actor.role },
        op_sc_id,
        op_sta_id:  actor.op_sta_id  ?? null,
        op_sta_name: actor.op_sta_name ?? null,
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

// ------------------------------
// FINISH (tf_rs_code 1/2/3 => log ลง t_transfer ทุกเคส)
// ------------------------------
exports.finishOpScan = async (req, res) => {
  const actor = actorOf(req);

  if (actor.u_type !== "op") return forbid(res, "Forbidden: u_type must be op", actor);
  if (actor.clientType !== "HH") return forbid(res, "Forbidden: clientType must be HH", actor);
  if (!actor.u_id) return res.status(401).json({ message: "Unauthorized", actor });

  const op_sc_id     = String(req.body.op_sc_id || "").trim();
  const good_qty_raw  = Number(req.body.good_qty);
  const scrap_qty_raw = Number(req.body.scrap_qty);

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

  const tf_rs_code = Number(req.body.tf_rs_code || 0);
  if (![1, 2, 3].includes(tf_rs_code)) {
    return res.status(400).json({ message: "tf_rs_code must be 1, 2, or 3", actor });
  }

  const SAFE_TRANSFER = safeTableName(process.env.TRANSFER_TABLE  || "dbo.t_transfer");
  const SAFE_PART     = safeTableName(process.env.PART_TABLE       || "dbo.part");
  const SAFE_RUNLOG   = safeTableName(process.env.TKRUNLOG_TABLE   || "dbo.TKRunLog");

  const normalizeSplits = (body) => {
    if (Array.isArray(body?.splits) && body.splits.length > 0) {
      return body.splits.map((x) => ({
        out_part_no: String(x?.out_part_no || "").trim(),
        qty:         Number(x?.qty),
      }));
    }
    return [];
  };

  const normalizeMergeLots = (body) => {
    if (Array.isArray(body?.merge_lots) && body.merge_lots.length > 0) {
      return body.merge_lots.map((x) => ({
        from_lot_no: String(x?.from_lot_no || "").trim(),
        qty:         Number(x?.qty),
      }));
    }
    return [];
  };

  const splits      = normalizeSplits(req.body);
  const merge_lots  = normalizeMergeLots(req.body);
  const out_part_no = String(req.body.out_part_no || "").trim();

  // --- validation by tf_rs_code ---
  if (tf_rs_code === 1) {
    if (!out_part_no)
      return res.status(400).json({ message: "out_part_no is required when tf_rs_code=1", actor });
  }

  if (tf_rs_code === 2) {
    if (splits.length === 0)
      return res.status(400).json({ message: "splits[] is required when tf_rs_code=2", actor });
    for (const s of splits) {
      if (!s.out_part_no)
        return res.status(400).json({ message: "out_part_no is required in each split", actor });
      if (!Number.isFinite(s.qty) || s.qty <= 0)
        return res.status(400).json({ message: "qty must be > 0 in each split", actor });
    }
    const sumQty = splits.reduce((acc, s) => acc + Math.trunc(s.qty), 0);
    if (sumQty !== Math.trunc(good_qty)) {
      return res.status(400).json({
        message:        "Sum of splits qty must equal good_qty",
        actor,
        good_qty:       Math.trunc(good_qty),
        sum_splits_qty: sumQty,
      });
    }
  }

  if (tf_rs_code === 3) {
    if (!out_part_no)
      return res.status(400).json({ message: "out_part_no is required when tf_rs_code=3", actor });
    if (merge_lots.length === 0)
      return res.status(400).json({ message: "merge_lots[] is required when tf_rs_code=3", actor });
    for (const m of merge_lots) {
      if (!m.from_lot_no)
        return res.status(400).json({ message: "from_lot_no is required in each merge_lot", actor });
      if (!Number.isFinite(m.qty) || m.qty <= 0)
        return res.status(400).json({ message: "qty must be > 0 in each merge_lot", actor });
    }
    const sumQty = merge_lots.reduce((acc, m) => acc + Math.trunc(m.qty), 0);
    if (sumQty !== Math.trunc(good_qty)) {
      return res.status(400).json({
        message:       "Sum of merge_lots qty must equal good_qty",
        actor,
        good_qty:      Math.trunc(good_qty),
        sum_merge_qty: sumQty,
      });
    }
  }

  console.log(
    `[OPSCAN_FINISH][REQ] u_id=${actor.u_id} u_name=${actor.u_name} op_sc_id=${op_sc_id} tf_rs_code=${tf_rs_code} good=${Math.trunc(good_qty)} scrap=${Math.trunc(scrap_qty)}`
  );

  try {
    const pool = await getPool();
    const tx   = new sql.Transaction(pool);
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // helpers
    const getLotNoByTkId = async (tx2, tkId) => {
      const r = await new sql.Request(tx2)
        .input("tk_id", sql.VarChar(20), tkId)
        .query(`SELECT TOP 1 lot_no FROM ${SAFE_TKDETAIL} WITH (NOLOCK) WHERE tk_id=@tk_id`);
      return r.recordset?.[0]?.lot_no ? String(r.recordset[0].lot_no) : null;
    };

    const lotExistsInRunLog = async (tx2, tkId, lotNo) => {
      const r = await new sql.Request(tx2)
        .input("tk_id",  sql.VarChar(20),   tkId)
        .input("lot_no", sql.NVarChar(300),  lotNo)
        .query(`
          SELECT TOP 1 lot_no
          FROM ${SAFE_RUNLOG} WITH (NOLOCK)
          WHERE tk_id=@tk_id AND lot_no=@lot_no
        `);
      return !!r.recordset?.[0];
    };

    const getPartByNo = async (partNo) => {
      const pr = await new sql.Request(tx)
        .input("part_no", sql.VarChar(100), partNo)
        .query(`
          SELECT TOP 1 part_id, part_no, part_name
          FROM ${SAFE_PART} WITH (NOLOCK)
          WHERE part_no = @part_no
        `);
      return pr.recordset?.[0] ?? null;
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

      // 🔴 กัน finish ซ้ำ
      if (row.op_sc_finish_ts) {
        await tx.rollback();
        return res.status(409).json({
          message: "Already finished",
          actor:   { u_id: actor.u_id, u_name: actor.u_name, role: actor.role },
          op_sc_id,
        });
      }

      const master_tk_id = String(row.tk_id || "").trim();
      if (!master_tk_id) {
        await tx.rollback();
        return res.status(400).json({ message: "op_scan.tk_id is NULL", actor, op_sc_id });
      }

      // 🔴 เช็ค is_finished (กัน finish ถ้า STA007 จบแล้ว)
      const finishedR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), master_tk_id)
        .query(`
          SELECT TOP 1 op_sc_id
          FROM ${SAFE_OPSCAN} WITH (NOLOCK)
          WHERE tk_id         = @tk_id
            AND op_sta_id     = 'STA007'
            AND op_sc_finish_ts IS NOT NULL
        `);

      if (finishedR.recordset?.[0]) {
        await tx.rollback();
        return res.status(403).json({
          message: "This tk_id is already FINISHED at STA007. Cannot finish again.",
          actor,
          tk_id: master_tk_id,
        });
      }

      const base_lot_no =
        row.lot_no && String(row.lot_no).trim()
          ? String(row.lot_no).trim()
          : await getLotNoByTkId(tx, master_tk_id);

      if (!base_lot_no) {
        await tx.rollback();
        return res.status(400).json({ message: "Cannot resolve base lot_no", actor, tk_id: master_tk_id });
      }

      let output_lot_no = base_lot_no;
      let output_run_no = null;
      const created_children = [];

      // ----------------------------------------------------------------
      // tf_rs_code = 1  Master-ID
      // ----------------------------------------------------------------
      if (tf_rs_code === 1) {
        const outPart = await getPartByNo(out_part_no);
        if (!outPart) {
          await tx.rollback();
          return res.status(400).json({ message: "out_part_no not found", actor, out_part_no });
        }

        const sp = await new sql.Request(tx)
          .input("tk_id",            sql.VarChar(20), master_tk_id)
          .input("part_id",          sql.Int,         Number(outPart.part_id))
          .input("created_by_u_id",  sql.Int,         Number(actor.u_id))
          .output("run_no",          sql.Char(14))
          .output("lot_no",          sql.NVarChar(300))
          .execute("dbo.usp_TKRunLog_Create");

        output_run_no = sp.output.run_no;
        output_lot_no = sp.output.lot_no;
        if (!output_run_no || !output_lot_no)
          throw new Error("DB did not return run_no/lot_no from usp_TKRunLog_Create");

        await new sql.Request(tx)
          .input("tk_id",   sql.VarChar(20),   master_tk_id)
          .input("part_id", sql.Int,            Number(outPart.part_id))
          .input("lot_no",  sql.NVarChar(300),  String(output_lot_no))
          .query(`
            UPDATE ${SAFE_TKDETAIL}
            SET part_id=@part_id, lot_no=@lot_no
            WHERE tk_id=@tk_id
          `);

        await new sql.Request(tx)
          .input("from_tk_id",      sql.VarChar(20),  master_tk_id)
          .input("to_tk_id",        sql.VarChar(20),  master_tk_id)
          .input("from_lot_no",     sql.NVarChar(300), base_lot_no)
          .input("to_lot_no",       sql.NVarChar(300), String(output_lot_no))
          .input("tf_rs_code",      sql.Int,           1)
          .input("transfer_qty",    sql.Int,           Math.trunc(good_qty))
          .input("op_sc_id",        sql.Char(12),      op_sc_id)
          .input("MC_id",           sql.VarChar(10),   row.MC_id ? String(row.MC_id).trim() : null)
          .input("created_by_u_id", sql.Int,           Number(actor.u_id))
          .input("transfer_ts",     sql.DateTime2(3),  now)
          .query(`
            INSERT INTO ${SAFE_TRANSFER}
              (from_tk_id, to_tk_id, from_lot_no, to_lot_no,
               tf_rs_code, transfer_qty,
               op_sc_id, MC_id, created_by_u_id, transfer_ts)
            VALUES
              (@from_tk_id, @to_tk_id, @from_lot_no, @to_lot_no,
               @tf_rs_code, @transfer_qty,
               @op_sc_id, @MC_id, @created_by_u_id, @transfer_ts)
          `);

        created_children.push({
          tk_id:         master_tk_id,
          run_no:        String(output_run_no).trim(),
          lot_no:        String(output_lot_no),
          out_part_no:   String(outPart.part_no),
          out_part_name: outPart.part_name ? String(outPart.part_name) : null,
          qty:           Math.trunc(good_qty),
        });

        console.log(`[OPSCAN_FINISH][MASTER][OK] op_sc_id=${op_sc_id} tk_id=${master_tk_id} from_lot=${base_lot_no} to_lot=${output_lot_no}`);
      }

      // ----------------------------------------------------------------
      // tf_rs_code = 2  Split-ID
      // ----------------------------------------------------------------
      if (tf_rs_code === 2) {
        for (const s of splits) {
          const outPart = await getPartByNo(s.out_part_no);
          if (!outPart) {
            await tx.rollback();
            return res.status(400).json({ message: "out_part_no not found", actor, out_part_no: s.out_part_no });
          }

          const sp = await new sql.Request(tx)
            .input("tk_id",           sql.VarChar(20), master_tk_id)
            .input("part_id",         sql.Int,         Number(outPart.part_id))
            .input("created_by_u_id", sql.Int,         Number(actor.u_id))
            .output("run_no",         sql.Char(14))
            .output("lot_no",         sql.NVarChar(300))
            .execute("dbo.usp_TKRunLog_Create");

          const run_no       = sp.output.run_no;
          const child_lot_no = sp.output.lot_no;
          if (!run_no || !child_lot_no)
            throw new Error("DB did not return run_no/lot_no from usp_TKRunLog_Create");

          await new sql.Request(tx)
            .input("from_tk_id",      sql.VarChar(20),  master_tk_id)
            .input("to_tk_id",        sql.VarChar(20),  master_tk_id)
            .input("from_lot_no",     sql.NVarChar(300), base_lot_no)
            .input("to_lot_no",       sql.NVarChar(300), String(child_lot_no))
            .input("tf_rs_code",      sql.Int,           2)
            .input("transfer_qty",    sql.Int,           Math.trunc(s.qty))
            .input("op_sc_id",        sql.Char(12),      op_sc_id)
            .input("MC_id",           sql.VarChar(10),   row.MC_id ? String(row.MC_id).trim() : null)
            .input("created_by_u_id", sql.Int,           Number(actor.u_id))
            .input("transfer_ts",     sql.DateTime2(3),  now)
            .query(`
              INSERT INTO ${SAFE_TRANSFER}
                (from_tk_id, to_tk_id, from_lot_no, to_lot_no,
                 tf_rs_code, transfer_qty,
                 op_sc_id, MC_id, created_by_u_id, transfer_ts)
              VALUES
                (@from_tk_id, @to_tk_id, @from_lot_no, @to_lot_no,
                 @tf_rs_code, @transfer_qty,
                 @op_sc_id, @MC_id, @created_by_u_id, @transfer_ts)
            `);

          created_children.push({
            tk_id:         master_tk_id,
            run_no:        String(run_no).trim(),
            lot_no:        String(child_lot_no),
            out_part_no:   String(outPart.part_no),
            out_part_name: outPart.part_name ? String(outPart.part_name) : null,
            qty:           Math.trunc(s.qty),
          });
        }

        console.log(`[OPSCAN_FINISH][SPLIT][OK] op_sc_id=${op_sc_id} tk_id=${master_tk_id} from_lot=${base_lot_no} children=${created_children.length}`);
      }

      // ----------------------------------------------------------------
      // tf_rs_code = 3  Co-ID (Merge)
      // ----------------------------------------------------------------
      if (tf_rs_code === 3) {
        const outPart = await getPartByNo(out_part_no);
        if (!outPart) {
          await tx.rollback();
          return res.status(400).json({ message: "out_part_no not found", actor, out_part_no });
        }

        // validate ทุก from_lot_no ต้องมีอยู่ใน TKRunLog ของ tk_id นี้
        for (const m of merge_lots) {
          const ok = await lotExistsInRunLog(tx, master_tk_id, m.from_lot_no);
          if (!ok) {
            await tx.rollback();
            return res.status(400).json({
              message:     "from_lot_no not found in TKRunLog for this tk_id",
              actor,
              tk_id:       master_tk_id,
              from_lot_no: m.from_lot_no,
            });
          }
        }

        const sp = await new sql.Request(tx)
          .input("tk_id",           sql.VarChar(20), master_tk_id)
          .input("part_id",         sql.Int,         Number(outPart.part_id))
          .input("created_by_u_id", sql.Int,         Number(actor.u_id))
          .output("run_no",         sql.Char(14))
          .output("lot_no",         sql.NVarChar(300))
          .execute("dbo.usp_TKRunLog_Create");

        output_run_no = sp.output.run_no;
        output_lot_no = sp.output.lot_no;
        if (!output_run_no || !output_lot_no)
          throw new Error("DB did not return run_no/lot_no from usp_TKRunLog_Create");

        await new sql.Request(tx)
          .input("tk_id",   sql.VarChar(20),  master_tk_id)
          .input("part_id", sql.Int,          Number(outPart.part_id))
          .input("lot_no",  sql.NVarChar(300), String(output_lot_no))
          .query(`
            UPDATE ${SAFE_TKDETAIL}
            SET part_id=@part_id, lot_no=@lot_no
            WHERE tk_id=@tk_id
          `);

        for (const m of merge_lots) {
          await new sql.Request(tx)
            .input("from_tk_id",      sql.VarChar(20),  master_tk_id)
            .input("to_tk_id",        sql.VarChar(20),  master_tk_id)
            .input("from_lot_no",     sql.NVarChar(300), m.from_lot_no)
            .input("to_lot_no",       sql.NVarChar(300), String(output_lot_no))
            .input("tf_rs_code",      sql.Int,           3)
            .input("transfer_qty",    sql.Int,           Math.trunc(m.qty))
            .input("op_sc_id",        sql.Char(12),      op_sc_id)
            .input("MC_id",           sql.VarChar(10),   row.MC_id ? String(row.MC_id).trim() : null)
            .input("created_by_u_id", sql.Int,           Number(actor.u_id))
            .input("transfer_ts",     sql.DateTime2(3),  now)
            .query(`
              INSERT INTO ${SAFE_TRANSFER}
                (from_tk_id, to_tk_id, from_lot_no, to_lot_no,
                 tf_rs_code, transfer_qty,
                 op_sc_id, MC_id, created_by_u_id, transfer_ts)
              VALUES
                (@from_tk_id, @to_tk_id, @from_lot_no, @to_lot_no,
                 @tf_rs_code, @transfer_qty,
                 @op_sc_id, @MC_id, @created_by_u_id, @transfer_ts)
            `);
        }

        created_children.push({
          tk_id:         master_tk_id,
          run_no:        String(output_run_no).trim(),
          lot_no:        String(output_lot_no),
          out_part_no:   String(outPart.part_no),
          out_part_name: outPart.part_name ? String(outPart.part_name) : null,
          qty:           Math.trunc(good_qty),
        });

        console.log(`[OPSCAN_FINISH][COID][OK] op_sc_id=${op_sc_id} tk_id=${master_tk_id} to_lot=${output_lot_no} inputs=${merge_lots.length}`);
      }

      // ----------------------------------------------------------------
      // 🔴 update op_scan finish + op_sta_id (กัน NULL ถ้า start เก่า)
      // ----------------------------------------------------------------
      const op_scan_lot_no = tf_rs_code === 2 ? base_lot_no : output_lot_no;

      await new sql.Request(tx)
        .input("op_sc_id",  sql.Char(12),      op_sc_id)
        .input("total_qty", sql.Int,            Math.trunc(total_qty))
        .input("good_qty",  sql.Int,            Math.trunc(good_qty))
        .input("scrap_qty", sql.Int,            Math.trunc(scrap_qty))
        .input("tf_rs_code",sql.Int,            tf_rs_code)
        .input("lot_no",    sql.NVarChar(300),  String(op_scan_lot_no))
        .input("op_sta_id", sql.VarChar(20),    actor.op_sta_id ?? null)
        .input("finish_ts", sql.DateTime2(3),   now)
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

      // 🔴 update TKHead tk_status
      // ถ้า finish ที่ STA007 → status = 1 (FINISHED)
      // ถ้า finish station อื่น → status = 2 (PARTIAL_DONE)
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

      await tx.commit();

      console.log(
        `[OPSCAN_FINISH][OK] op_sc_id=${op_sc_id} tk_id=${master_tk_id} op_sta_id=${actor.op_sta_id ?? "-"} tf_rs_code=${tf_rs_code} total=${Math.trunc(total_qty)} good=${Math.trunc(good_qty)} scrap=${Math.trunc(scrap_qty)} tk_status=${newTkStatus}`
      );

      return res.json({
        message: "Finished",
        actor:   { u_id: actor.u_id, u_name: actor.u_name, role: actor.role },

        op_sc_id,
        tk_id:          master_tk_id,
        op_sta_id:      actor.op_sta_id  ?? null,
        op_sta_name:    actor.op_sta_name ?? null,
        MC_id:          row.MC_id ?? null,
        lot_no:         op_scan_lot_no,

        op_sc_total_qty: Math.trunc(total_qty),
        op_sc_good_qty:  Math.trunc(good_qty),
        op_sc_scrap_qty: Math.trunc(scrap_qty),
        tf_rs_code,

        tk_status:      newTkStatus,
        is_finished:    isFinishAtSTA007,

        out_part_no: out_part_no || "",
        created_children_count: created_children.length,
        created_children,

        op_sc_ts:       row.op_sc_ts ? new Date(row.op_sc_ts).toISOString() : null,
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
