// controllers/tray.controller.js
const crypto = require("crypto");
const { sql, getPool } = require("../config/db");

function yyyymmdd(date = new Date()) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}${m}${d}`;
}

async function generateRunningId(tx, tableName, idCol, prefix) {
  // Example: prefix = "TR20260202"
  const last = await new sql.Request(tx)
    .input("likePrefix", sql.VarChar(50), `${prefix}%`)
    .query(`
      SELECT TOP 1 ${idCol} AS lastId
      FROM ${tableName} WITH (UPDLOCK, HOLDLOCK)
      WHERE ${idCol} LIKE @likePrefix
      ORDER BY ${idCol} DESC
    `);

  let running = 1;
  if (last.recordset.length > 0) {
    const lastId = String(last.recordset[0].lastId);
    const tail = lastId.slice(prefix.length);
    const n = parseInt(tail, 10);
    if (!Number.isNaN(n)) running = n + 1;
  }

  return `${prefix}${String(running).padStart(4, "0")}`;
}

/**
 * POST /api/trays
 * body: { part_id, qty }
 * role: admin/manager
 */
async function createTray(req, res) {
  const partId = Number(req.body.part_id);
  const qty = Number(req.body.qty);

  if (!Number.isFinite(partId)) {
    return res.status(400).json({ message: "part_id is required (number)" });
  }
  if (!Number.isFinite(qty) || qty <= 0) {
    return res.status(400).json({ message: "qty must be a number > 0" });
  }

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    // Find OP1 station by seq=1
    const op1 = await pool.request().query(`
      SELECT TOP 1 op_sta_id, op_sta_code, op_sta_name, op_sta_seq
      FROM op_station
      WHERE op_sta_seq = 1
      ORDER BY op_sta_id ASC
    `);

    if (op1.recordset.length === 0) {
      return res.status(500).json({ message: "OP1 station not found in op_station (op_sta_seq=1)" });
    }

    const op1Row = op1.recordset[0];

    // Validate part exists
    const part = await pool
      .request()
      .input("part_id", sql.Int, partId)
      .query(`
        SELECT TOP 1 part_id, part_no, part_name
        FROM part
        WHERE part_id = @part_id
      `);

    if (part.recordset.length === 0) {
      return res.status(404).json({ message: "part not found" });
    }

    const now = new Date();
    const prefix = `TR${yyyymmdd(now)}`;

    const tx = new sql.Transaction(pool);
    await tx.begin();

    try {
      const t_id = await generateRunningId(tx, "tray", "t_id", prefix);

      // QR code (simple + guaranteed unique if t_id unique)
      const rand = crypto.randomBytes(3).toString("hex").toUpperCase(); // 6 chars
      const t_qr_code = `${t_id}-${rand}`;

      // Default tray status (MVP)
      const t_status_code = Number(process.env.TRAY_STATUS_IN_PROGRESS || 1);

      await new sql.Request(tx)
        .input("t_id", sql.VarChar(30), t_id)
        .input("t_qr_code", sql.NVarChar(100), t_qr_code)
        .input("part_id", sql.Int, partId)
        .input("t_current_qty", sql.Int, qty)
        .input("t_created_op_sta_id", sql.Int, op1Row.op_sta_id)
        .input("t_current_op_sta_id", sql.Int, op1Row.op_sta_id)
        .input("t_status_code", sql.Int, t_status_code)
        .input("t_created_by", sql.Int, Number(req.user?.u_id || 0))
        .input("t_created_ts", sql.DateTime2(3), now)
        .query(`
          INSERT INTO tray (
            t_id, t_qr_code, part_id,
            t_current_qty, t_created_op_sta_id, t_current_op_sta_id,
            t_status_code, t_created_by, t_created_ts
          )
          VALUES (
            @t_id, @t_qr_code, @part_id,
            @t_current_qty, @t_created_op_sta_id, @t_current_op_sta_id,
            @t_status_code, @t_created_by, @t_created_ts
          )
        `);

      await tx.commit();

      return res.status(201).json({
        message: "Tray created",
        tray: {
          t_id,
          t_qr_code,
          part: part.recordset[0],
          current_qty: qty,
          current_station: {
            op_sta_id: op1Row.op_sta_id,
            op_sta_code: op1Row.op_sta_code,
            op_sta_name: op1Row.op_sta_name,
            op_sta_seq: op1Row.op_sta_seq,
          },
          t_status_code,
          created_by: req.user?.u_id,
          created_ts: now.toISOString(),
        },
      });
    } catch (e) {
      await tx.rollback();
      if (String(e?.message || "").toLowerCase().includes("unique")) {
        return res.status(409).json({ message: "Duplicate tray id/qr (try again)" });
      }
      throw e;
    }
  } catch (err) {
    console.error("CREATE TRAY ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

/**
 * GET /api/trays/by-qr/:qr
 * role: any authenticated (operator/admin/manager)
 */
async function getTrayByQr(req, res) {
  const qr = String(req.params.qr || "").trim();
  if (!qr) return res.status(400).json({ message: "qr is required" });

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const result = await pool
      .request()
      .input("qr", sql.NVarChar(100), qr)
      .query(`
        SELECT TOP 1
          t.t_id,
          t.t_qr_code,
          t.t_current_qty,
          t.t_status_code,
          t.part_id,
          p.part_no,
          p.part_name,
          t.t_current_op_sta_id,
          s.op_sta_code,
          s.op_sta_name,
          s.op_sta_seq
        FROM tray t
        JOIN part p ON p.part_id = t.part_id
        JOIN op_station s ON s.op_sta_id = t.t_current_op_sta_id
        WHERE t.t_qr_code = @qr
      `);

    if (result.recordset.length === 0) {
      return res.status(404).json({ message: "Tray not found" });
    }

    const r = result.recordset[0];

    return res.json({
      tray: {
        t_id: r.t_id,
        t_qr_code: r.t_qr_code,
        part: { part_id: r.part_id, part_no: r.part_no, part_name: r.part_name },
        current_qty: r.t_current_qty,
        t_status_code: r.t_status_code,
        current_station: {
          op_sta_id: r.t_current_op_sta_id,
          op_sta_code: r.op_sta_code,
          op_sta_name: r.op_sta_name,
          op_sta_seq: r.op_sta_seq,
        },
      },
    });
  } catch (err) {
    console.error("GET TRAY BY QR ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

module.exports = { createTray, getTrayByQr };
